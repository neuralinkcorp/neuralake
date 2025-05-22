# mypy: disable-error-code=override

from os import path
from typing import Any, Optional, Sequence

import boto3
import polars as pl

from neuralake.core.dataframe import NlkDataFrame
from neuralake.core.tables.filters import (
    InputFilters,
    NormalizedFilters,
    normalize_filters,
)
from neuralake.core.tables.metadata import (
    TableMetadata,
    TableProtocol,
    TableSchema,
)
from neuralake.core.tables.util import (
    Filter,
    Partition,
    PartitioningScheme,
    RoapiOptions,
    exactly_one_equality_filter,
    get_storage_options,
)


def pl_all(exprs: Sequence[pl.Expr]) -> pl.Expr:
    assert len(exprs) > 0

    result = exprs[0]
    for expr in exprs[1:]:
        result = result & expr

    return result


def pl_any(exprs: Sequence[pl.Expr]) -> pl.Expr:
    assert len(exprs) > 0

    result = exprs[0]
    for expr in exprs[1:]:
        result = result | expr

    return result


def _filters_to_conjunction_expr(filters: list[Filter]) -> pl.Expr | None:
    if not filters:
        return None

    result = _filter_to_expr(filters[0])
    for filter in filters[1:]:
        result = result & _filter_to_expr(filter)

    return pl_all([_filter_to_expr(filter) for filter in filters])


def _filters_to_expr(filters: NormalizedFilters) -> pl.Expr | None:
    conjunctions = [_filters_to_conjunction_expr(filter_set) for filter_set in filters]
    not_none_conjunctions = [c for c in conjunctions if c is not None]
    if not not_none_conjunctions:
        return None

    return pl_any(not_none_conjunctions)


def _filter_to_expr(filter: Filter) -> pl.Expr:
    if filter.operator == "=":
        return pl.col(filter.column) == filter.value
    elif filter.operator == "!=":
        return pl.col(filter.column) != filter.value
    elif filter.operator == "<":
        return pl.col(filter.column) < filter.value
    elif filter.operator == ">":
        return pl.col(filter.column) > filter.value
    elif filter.operator == "<=":
        return pl.col(filter.column) <= filter.value
    elif filter.operator == ">=":
        return pl.col(filter.column) >= filter.value
    elif filter.operator == "in":
        return pl.col(filter.column).is_in(filter.value)
    elif filter.operator == "not in":
        return ~pl.col(filter.column).is_in(filter.value)
    elif filter.operator == "contains":
        return pl.col(filter.column).str.contains(filter.value)
    elif filter.operator == "includes":
        return pl.col(filter.column).list.contains(filter.value)
    elif filter.operator == "includes any":
        return pl_any(
            [pl.col(filter.column).list.contains(value) for value in filter.value]
        )
    elif filter.operator == "includes all":
        return pl_all(
            [pl.col(filter.column).list.contains(value) for value in filter.value]
        )
    else:
        raise ValueError(f"Unsupported operator {filter.operator}")


class ParquetTable(TableProtocol):
    def __init__(
        self,
        name: str,
        uri: str,
        partitioning: list[Partition],
        partitioning_scheme: PartitioningScheme = PartitioningScheme.DIRECTORY,
        description: str = "",
        docs_filters: list[Filter] = [],
        docs_columns: list[str] | None = None,
        roapi_opts: RoapiOptions | None = None,
        parquet_file_name: str = "df.parquet",
        table_metadata_args: dict[str, Any] | None = None,
    ):
        if not isinstance(partitioning_scheme, PartitioningScheme):
            raise ValueError(f"Invalid parititioning scheme, got {partitioning_scheme}")

        self.name = name
        self.uri = uri
        self.partitioning = partitioning
        self.partitioning_scheme = partitioning_scheme

        self.table_metadata = TableMetadata(
            table_type="PARQUET",
            description=description,
            docs_args={"filters": docs_filters, "columns": docs_columns},
            roapi_opts=roapi_opts,
            **(table_metadata_args or {}),
        )

        self.parquet_file_name = parquet_file_name

    def get_schema(self) -> TableSchema:
        partitions = [
            {
                "column_name": filter.column,
                "type_annotation": type(filter.value).__name__,
                "value": filter.value,
            }
            for filter in self.table_metadata.docs_args.get("filters", [])
        ]

        # Pop the selected columns so that we still pull the full schema below
        docs_args = {**self.table_metadata.docs_args}
        docs_args.pop("columns")

        columns = None
        if docs_args or not partitions:
            table: NlkDataFrame = self(**docs_args)
            columns = [
                {
                    "name": key,
                    "type": type.__str__(),
                }
                for key, type in table.schema.items()
            ]

        return TableSchema(partitions=partitions, columns=columns)

    def __call__(
        self,
        filters: InputFilters | None = None,
        columns: Optional[list[str]] = None,
        boto3_session: boto3.Session | None = None,
        endpoint_url: str | None = None,
        **kwargs: Any,
    ) -> NlkDataFrame:
        normalized_filters = normalize_filters(filters)
        uri, remaining_partitions, remaining_filters, applied_filters = (
            self._build_uri_from_filters(normalized_filters)
        )

        storage_options = get_storage_options(
            boto3_session=boto3_session,
            endpoint_url=endpoint_url,
        )

        df = pl.scan_parquet(
            uri,
            hive_partitioning=len(remaining_partitions) > 0,
            hive_schema={
                partition.column: partition.col_type
                for partition in remaining_partitions
            },
            allow_missing_columns=True,
            storage_options=storage_options,
        )

        if applied_filters:
            # Add columns removed from partitions and added to uri
            df = df.with_columns(
                pl.lit(f.value)
                .cast(
                    next(
                        partition.col_type
                        for partition in self.partitioning
                        if partition.column == f.column
                    )
                )
                .alias(f.column)
                for f in applied_filters
            )

        if remaining_filters:
            filter_expr = _filters_to_expr(remaining_filters)
            if filter_expr is not None:
                df = df.filter(filter_expr)

        if columns:
            df = df.select(columns)

        return NlkDataFrame(frame=df)

    def build_file_fragment(self, filters: list[Filter]) -> str:
        """
        Returns a file path from the base table URI with the given filters.
        This will raise an error if the filter does not specify all partitions.

        This is currently used to generate the file path used by ROAPI to infer schemas.
        """
        uri_with_prefix, partitions, _, _ = self._build_uri_from_filters(
            normalize_filters(filters), include_base_uri=False
        )
        if len(partitions) > 0:
            partition_names = [partition.column for partition in partitions]
            raise ValueError(
                f"Not enough partitions specified, missing: {partition_names}"
            )

        return path.join(uri_with_prefix, self.parquet_file_name)

    def _build_uri_from_filters(
        self,
        filters: NormalizedFilters,
        include_base_uri: bool = True,
    ) -> tuple[str, list[Partition], NormalizedFilters, list[Filter]]:
        """Attempts to build an S3 list prefix from the given filters.
        We do this because pyarrow runs an S3 List() query on the base URI
        before applying filters to possible files. This can be slow.

        We can force pyarrow to do more sophisticated prefix filtering
        if we pre-construct the URI before making the call. If a partition
        has exactly one filter that uses strict equality, we know that
        it will contain that filter string in the URI. We attempt to do this
        for each partition filter, in order, until we encounter a
        partition that:
            1. does not have a filter, or
            2. has a more than one filters, or
            3. has filters that are not strict equality checks.

        This gives us the longest URL that can be used as a prefix filter for
        all files returned by the S3 List() call.

        In a benchmark, this brought reading ~1 million rows of binned spike files
        from 12s to 1.5s.

        In the future, we can further optimize this by building a list of
        candidate URIs and doing separate prefix filtering with each of those, in parallel.

        Long-term we should push this logic into pyarrow and make an upstream commit.

        NOTE: Add trailing slash - this is important to ensure that,
        when partitioning only by implant ID, a future 5-digit implant beginning with the same 4 digits
        as a 4-digit implant is not included in a 4-digit implant's query.
        """
        uri = self.uri if include_base_uri else ""

        if not filters or not self.partitioning:
            return uri, self.partitioning, filters, []

        partitions = list(self.partitioning)
        filters = [list(filter_set) for filter_set in filters]
        applied_filters = []

        for i, partition in enumerate(self.partitioning):
            partition_filters = [
                exactly_one_equality_filter(partition, f) for f in filters
            ]

            # either 0 or multiple filters for this partition,
            # break and deal with the s3 list() query
            if any(partition_filter is None for partition_filter in partition_filters):
                break

            # Only move forward if all remaining filter sets have the same partition filter
            if not all(
                partition_filter == partition_filters[0]
                for partition_filter in partition_filters
            ):
                break

            partition_filter = partition_filters[0]

            if self.partitioning_scheme == PartitioningScheme.DIRECTORY:
                partition_component = str(partition_filter.value)
            elif self.partitioning_scheme == PartitioningScheme.HIVE:
                partition_component = (
                    partition.column + "=" + str(partition_filter.value)
                )

            uri = path.join(uri, partition_component)

            # remove the partition and filter since it's been applied
            # technically might not need to remove the partitions,
            # but they are semantically meaningless as we already have
            # constructed the URI, so we can pop them
            partitions.remove(partition)
            for filter_set in filters:
                filter_set.remove(partition_filter)

            applied_filters.append(partition_filter)

        uri = path.join(
            uri, ""
        )  # trailing slash prevents inclusion of partitions that are subsets of other partitions

        return (uri, partitions, filters, applied_filters)
