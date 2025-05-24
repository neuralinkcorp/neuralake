# User Guide

## Core concepts

### Tables

A table in Neuralake is a Python function that returns an `NlkDataFrame`. An `NlkDataFrame` is a thin wrapper of the [polars LazyFrame](https://docs.pola.rs/py-polars/html/reference/lazyframe/index.html). Tables are the fundamental building blocks for accessing and querying data. Tables can be backed by [DeltaLake](https://delta.io/) tables, [Parquet tables](https://parquet.apache.org/), or pure Python functions.

#### DeltaLake tables
```python
from neuralake.core.tables import DeltalakeTable
import pyarrow as pa

# Define the schema
schema = pa.schema([
    ("s_suppkey", pa.int64()),
    ("s_name", pa.string()),
    ("s_address", pa.string()),
    ("s_nationkey", pa.int64()),
    ("s_phone", pa.string()),
    ("s_acctbal", pa.decimal128(12, 2)),
    ("s_comment", pa.string()),
])

# Create the table
supplier = DeltalakeTable(
    name="supplier",
    uri="s3://my-bucket/tpc-h/supplier/",
    schema=schema,
    description="Supplier information from the TPC-H benchmark",
    unique_columns=["s_suppkey"],
)
```

#### Parquet tables
```python
from neuralake.core.tables import ParquetTable, Partition, PartitioningScheme
import pyarrow as pa

# Define the schema
schema = pa.schema([
    ("ps_partkey", pa.int64()),
    ("ps_suppkey", pa.int64()),
    ("ps_availqty", pa.int32()),
    ("ps_supplycost", pa.decimal128(12, 2)),
    ("ps_comment", pa.string()),
])

# Create the table
partsupp = ParquetTable(
    name="partsupp",
    uri="s3://my-bucket/tpc-h/partsupp",
    schema=schema,
    description="Part supplier relationship information from the TPC-H benchmark",
)
```

#### Function tables
Function tables are created using the `@table` decorator and allow you to define custom data access logic:

```python
from neuralake.core.tables import table
import polars as pl

@table
def supplier_sample() -> NlkDataFrame:
    """A custom table that returns sample supplier data."""
    data = {
        "s_suppkey": [1, 2, 3, 4, 5],
        "s_name": ["Supplier#1", "Supplier#2", "Supplier#3", "Supplier#4", "Supplier#5"],
        "s_address": ["123 Main St", "456 Oak Ave", "789 Pine Rd", "321 Elm St", "654 Maple Dr"],
        "s_nationkey": [1, 1, 2, 2, 3],
        "s_phone": ["555-0001", "555-0002", "555-0003", "555-0004", "555-0005"],
        "s_acctbal": [1000.00, 2000.00, 3000.00, 4000.00, 5000.00],
        "s_comment": ["Comment 1", "Comment 2", "Comment 3", "Comment 4", "Comment 5"]
    }
    return NlkDataFrame(frame=pl.LazyFrame(data))
```

### Databases

A Neuralake database is a Python module that contains tables. There are two main ways to create databases:

#### Module database
A module database wraps a Python module containing table definitions:

```python
# my_database.py
from neuralake.core.tables import table

@table
def supplier():
    """Supplier information."""
    return NlkDataFrame(...)

@table
def partsupp():
    """Part supplier relationship information."""
    return NlkDataFrame(...)

# Using the database
from neuralake.core.catalog import ModuleDatabase
import my_database

db = ModuleDatabase(my_database)

# Query data
>>> df = db.supplier()
>>> df.head()
shape: (5, 7)
┌──────────┬───────────┬────────────┬────────────┬──────────┬──────────┬──────────┐
│ s_suppkey│ s_name    │ s_address  │ s_nationkey│ s_phone  │ s_acctbal│ s_comment│
├──────────┼───────────┼────────────┼────────────┼──────────┼──────────┼──────────┤
│ 1        │ Supplier#1│ 123 Main St│ 1          │ 555-0001 │ 1000.00  │ Comment 1│
│ 2        │ Supplier#2│ 456 Oak Ave│ 1          │ 555-0002 │ 2000.00  │ Comment 2│
│ 3        │ Supplier#3│ 789 Pine Rd│ 2          │ 555-0003 │ 3000.00  │ Comment 3│
│ 4        │ Supplier#4│ 321 Elm St │ 2          │ 555-0004 │ 4000.00  │ Comment 4│
│ 5        │ Supplier#5│ 654 Maple Dr│ 3         │ 555-0005 │ 5000.00  │ Comment 5│
└──────────┴───────────┴────────────┴────────────┴──────────┴──────────┴──────────┘
```

#### Custom database
You can also create custom databases by implementing the `Database` protocol:

```python
from neuralake.core.catalog import Database, TableProtocol

class MyDatabase(Database):
    def __init__(self):
        self._tables = {
            "supplier": DeltalakeTable(...),
            "partsupp": ParquetTable(...),
        }

    def get_tables(self, show_deprecated: bool = False) -> dict[str, TableProtocol]:
        return self._tables

    def table(self, name: str, *args, **kwargs) -> NlkDataFrame:
        return self._tables[name](*args, **kwargs)
```

### Catalogs

A catalog is a Python module that is a collection of databases.

```python
from neuralake.core.catalog import Catalog, ModuleDatabase
import supplier_data
import partsupp_data

# Create a catalog
dbs = {
    "supplier_data": ModuleDatabase(supplier_data),
    "partsupp_data": ModuleDatabase(partsupp_data),
}

MyCatalog = Catalog(dbs)

# Query data across databases
>>> supplier = MyCatalog.db("supplier_data").supplier()
>>> partsupp = MyCatalog.db("partsupp_data").partsupp()

# Join data across databases
>>> joined = supplier.join(partsupp, left_on="s_suppkey", right_on="ps_suppkey")
>>> joined.head()
shape: (5, 12)
┌──────────┬───────────┬────────────┬────────────┬──────────┬──────────┬──────────┬──────────┬──────────┬────────────┬─────────────┬────────── ┐
│ s_suppkey│ s_name    │ s_address  │ s_nationkey│ s_phone  │ s_acctbal│ s_comment│ps_partkey│ps_suppkey│ps_availqty │ps_supplycost│ps_comment │
├──────────┼───────────┼────────────┼────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼────────────┼─────────────┼───────────┤
│ 1        │ Supplier#1│ 123 Main St│ 1          │ 555-0001 │ 1000.00  │ Comment 1│ 1        │ 1        │ 100        │ 100.00      │ Part 1    │
│ 2        │ Supplier#2│ 456 Oak Ave│ 1          │ 555-0002 │ 2000.00  │ Comment 2│ 2        │ 2        │ 200        │ 200.00      │ Part 2    │
│ 3        │ Supplier#3│ 789 Pine Rd│ 2          │ 555-0003 │ 3000.00  │ Comment 3│ 3        │ 3        │ 300        │ 300.00      │ Part 3    │
│ 4        │ Supplier#4│ 321 Elm St │ 2          │ 555-0004 │ 4000.00  │ Comment 4│ 4        │ 4        │ 400        │ 400.00      │ Part 4    │
│ 5        │ Supplier#5│ 654 Maple D│  3         │ 555-0005 │ 5000.00  │ Comment 5│ 5        │ 5        │ 500        │ 500.00      │ Part 5    │
└──────────┴───────────┴────────────┴────────────┴──────────┴──────────┴──────────┴──────────┴──────────┴────────────┴─────────────┴───────────┘
```

## Querying data

Neuralake provides a consistent interface for querying data across all table types:

```python
# Filter data
>>> df = db.supplier(filters=[("s_nationkey", "=", 1)])

# Select columns
>>> df = db.supplier(columns=["s_suppkey", "s_name"])

# Complex queries
>>> df = db.supplier(
...     filters=[
...         ("s_nationkey", "=", 1),
...         ("s_acctbal", ">=", 1000.00),
...     ],
...     columns=["s_suppkey", "s_name", "s_acctbal"],
... )
```

## Advanced features

### Caching
DeltaLake tables support caching to improve performance:

```python
from neuralake.core.tables import DeltaCacheOptions

# Configure caching
cache_options = DeltaCacheOptions(
    file_cache_path="~/.neuralake/cache",
    file_cache_last_checkpoint_valid_duration="30m",
)

# Use caching
>>> df = db.supplier(cache_options=cache_options)
```

### Custom columns
You can add custom computed columns to tables:

```python
# Add a custom column
supplier = DeltalakeTable(
    name="supplier",
    uri="s3://my-bucket/tpc-h/supplier",
    schema=schema,
    extra_cols=[
        (pl.col("s_acctbal") * 1.1, "s_acctbal_with_tax"),
    ],
)

# Query with custom column
>>> df = supplier(columns=["s_suppkey", "s_acctbal_with_tax"])
```