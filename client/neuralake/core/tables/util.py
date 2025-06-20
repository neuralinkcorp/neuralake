from dataclasses import dataclass
from enum import Enum
from typing import Any, NamedTuple, Optional

import boto3
import polars as pl
import pyarrow as pa
from pypika import Field, CustomFunction, Criterion

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


class RawCriterion(Criterion):
    def __init__(self, expr: str) -> None:
        super().__init__()
        self.expr = expr

    def get_sql(self, **kwargs: Any) -> str:
        return self.expr


def filters_to_sql_predicate(
    schema: pa.Schema, filters: NormalizedFilters
) -> Criterion:
    return Criterion.any(
        filters_to_sql_conjunction(schema, filter_set) for filter_set in filters
    )


def filters_to_sql_conjunction(schema: pa.Schema, filters: list[Filter]) -> Criterion:
    return Criterion.all(filter_to_sql_expr(schema, f) for f in filters)


def filter_to_sql_expr(schema: pa.Schema, f: Filter) -> Criterion:
    column = f.column
    if column not in schema.names:
        raise ValueError(f"Invalid column name {column}")

    column_type = schema.field(column).type
    if f.operator == "=":
        return Field(column) == f.value
    elif f.operator == "!=":
        return Field(column) != f.value
    elif f.operator == "<":
        return Field(column) < f.value
    elif f.operator == "<=":
        return Field(column) <= f.value
    elif f.operator == ">":
        return Field(column) > f.value
    elif f.operator == ">=":
        return Field(column) >= f.value
    elif f.operator == "in":
        return Field(column).isin(f.value)
    elif f.operator == "not in":
        return Field(column).notin(f.value)
    elif f.operator == "contains":
        assert isinstance(f.value, str)
        return Field(column).like(f"%{f.value}%")

    elif f.operator in ("includes", "includes any", "includes all"):
        assert pa.types.is_list(column_type) or pa.types.is_large_list(column_type)

        values: list[Any]
        if f.operator == "includes":
            values = [f.value]
        else:
            assert isinstance(f.value, list | tuple)
            values = list(f.value)
            assert values

        # NOTE: for includes any/all, we join multiple array_contains with or/and
        array_contains = CustomFunction("array_contains", ["table", "value"])
        include_exprs = [array_contains(Field(column), value) for value in values]
        final_expr = (
            Criterion.all(include_exprs)
            if f.operator == "includes all"
            else Criterion.any(include_exprs)
        )

        return final_expr

    else:
        raise ValueError(f"Invalid operator {f.operator}")
