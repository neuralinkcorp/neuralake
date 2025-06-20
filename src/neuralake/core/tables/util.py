from dataclasses import dataclass
from enum import Enum
from typing import Any, NamedTuple, Optional

import boto3
import polars as pl
import pyarrow as pa

from neuralake.core.tables.filters import Filter, NormalizedFilters


@dataclass
class RoapiOptions:
    use_memory_table: bool = False
    disable: bool = False
    override_name: str | None = None
    reload_interval_seconds: int | None = None


@dataclass
class DeltaRoapiOptions(RoapiOptions):
    reload_interval_seconds: int | None = 60


class PartitioningScheme(Enum):
    """
    Defines the partitioning scheme for the table.

    DIRECTORY - e.g. s3://bucket/5956/2024-03-24
    HIVE - e.g. s3://bucket/implant_id=5956/date=2024-03-24
    """

    DIRECTORY = 1
    HIVE = 2


class Partition(NamedTuple):
    column: str
    col_type: pl.DataType


def exactly_one_equality_filter(
    partition: Partition, filters: list[Filter]
) -> Optional[Filter]:
    """Checks whether exactly one equality filter exists for the given partition"""
    match = None

    for f in filters:
        if f.column == partition.column:
            # Multiple matches found, or found comparison that is not equality operator
            if match is not None or f.operator != "=":
                return None
            # First match for full equality operator
            elif f.operator == "=":
                match = f

    return match


def get_storage_options(
    boto3_session: boto3.Session | None = None,
    endpoint_url: str | None = None,
) -> dict[str, str]:
    storage_options = {}

    if endpoint_url is not None:
        storage_options["aws_endpoint_url"] = endpoint_url

    if boto3_session is not None:
        creds = boto3_session.get_credentials()
        storage_options = {
            **storage_options,
            "aws_access_key_id": creds.access_key,
            "aws_secret_access_key": creds.secret_key,
            "aws_session_token": creds.token,
            "aws_region": boto3_session.region_name,
        }

    # Storage options passed to delta-rs need to be not null
    storage_options = {k: v for k, v in storage_options.items() if v}

    return storage_options


def get_pyarrow_filesystem_args(
    boto3_session: boto3.Session | None = None,
    endpoint_url: str | None = None,
) -> dict[str, str]:
    pyarrow_filesystem_args = {}

    if endpoint_url is not None:
        pyarrow_filesystem_args["endpoint_override"] = endpoint_url

    if boto3_session is not None:
        creds = boto3_session.get_credentials()
        pyarrow_filesystem_args = {
            **pyarrow_filesystem_args,
            "access_key": creds.access_key,
            "secret_key": creds.secret_key,
            "session_token": creds.token,
            "region": boto3_session.region_name,
        }

    pyarrow_filesystem_args = {
        k: v for k, v in pyarrow_filesystem_args.items() if v is not None
    }

    return pyarrow_filesystem_args


def filters_to_sql_predicate(schema: pa.Schema, filters: NormalizedFilters) -> str:
    if not filters:
        return "true"

    return " or ".join(
        filters_to_sql_conjunction(schema, filter_set) for filter_set in filters
    )


def filters_to_sql_conjunction(schema: pa.Schema, filters: list[Filter]) -> str:
    if not filters:
        return "true"

    exprs = (filter_to_sql_expr(schema, f) for f in filters)
    conjunction_expr = " and ".join(exprs)
    return f"({conjunction_expr})"


def filter_to_sql_expr(schema: pa.Schema, f: Filter) -> str:
    column = f.column
    if column not in schema.names:
        raise ValueError(f"Invalid column name {column}")

    column_type = schema.field(column).type
    if f.operator in (
        "=",
        "!=",
        "<",
        "<=",
        ">",
        ">=",
        "in",
        "not in",
    ):
        value_str = value_to_sql_expr(f.value, column_type)
        return f"({column} {f.operator} {value_str})"

    elif f.operator == "contains":
        assert isinstance(f.value, str)
        escaped_str = escape_str_for_sql(f.value)
        like_str = f"'%{escaped_str}%'"
        return f"({column} like {like_str})"

    elif f.operator in ("includes", "includes any", "includes all"):
        assert pa.types.is_list(column_type) or pa.types.is_large_list(column_type)

        values: list[Any]
        if f.operator == "includes":
            values = [f.value]
        else:
            assert isinstance(f.value, list | tuple)
            values = list(f.value)

        # NOTE: for includes any/all, we join multiple array_contains with or/and
        value_exprs = (
            value_to_sql_expr(value, column_type.value_type) for value in values
        )
        include_exprs = (
            f"array_contains({column}, {value_expr})" for value_expr in value_exprs
        )
        join_operator = " or " if f.operator == "includes any" else " and "
        conjunction_expr = join_operator.join(include_exprs)

        return f"({conjunction_expr})"

    else:
        raise ValueError(f"Invalid operator {f.operator}")


def value_to_sql_expr(value: Any, value_type: pa.DataType) -> str:
    if isinstance(value, list | tuple):
        elements_str = ", ".join(
            value_to_sql_expr(element, value_type) for element in value
        )
        value_str = f"({elements_str})"
    else:
        value_str = str(value)
        # Escape the string so the user doesn't need to filter like ("col", "=", "'value'")
        if pa.types.is_string(value_type):
            escaped_str = escape_str_for_sql(value_str)
            value_str = f"'{escaped_str}'"
    return value_str


def escape_str_for_sql(value: str) -> str:
    return value.replace("'", "''")
