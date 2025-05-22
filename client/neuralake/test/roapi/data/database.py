import polars as pl
import pyarrow as pa

from neuralake.core import (
    DeltalakeTable,
    Filter,
    NlkDataFrame,
    ParquetTable,
    Partition,
    PartitioningScheme,
    table,
)
from neuralake.core.tables.util import RoapiOptions


@table
def new_table() -> NlkDataFrame:
    return NlkDataFrame(frame=pl.LazyFrame({"a": [1, 2, 3], "b": [2, 4, 6]}))


test_parquet_table = ParquetTable(
    name="test_parquet_table",
    uri="s3://bucket/data/",
    partitioning=[Partition(column="implant_id", col_type=pa.int16())],
    docs_filters=[Filter("implant_id", "=", 4595)],
)

test_parquet_table = ParquetTable(
    name="test_parquet_table_hive",
    uri="s3://bucket/data/",
    partitioning=[Partition(column="implant_id", col_type=pa.int16())],
    partitioning_scheme=PartitioningScheme.HIVE,
    docs_filters=[Filter("implant_id", "=", 4595)],
)

test_delta_table = DeltalakeTable(
    name="test_delta_table",
    schema=pa.schema([]),
    uri="s3://bucket/data/",
)

test_delta_table_disable = DeltalakeTable(
    name="test_delta_table_disable",
    schema=pa.schema([]),
    uri="s3://bucket/data/",
    roapi_opts=RoapiOptions(disable=True),
)

test_delta_table_override_name = DeltalakeTable(
    name="test_delta_table_override_name",
    schema=pa.schema([]),
    uri="s3://bucket/data/",
    roapi_opts=RoapiOptions(override_name="new_name"),
)
