# User Guide

## Core concepts

### Tables

A table in Neuralake is a Python function that returns an `NlkDataFrame`. An `NlkDataFrame` is a thin wrapper of the [polars LazyFrame](https://docs.pola.rs/py-polars/html/reference/lazyframe/index.html). Tables are the fundamental building blocks for accessing and querying data. Tables can be backed by [DeltaLake](https://delta.io/) tables, [Parquet tables](https://parquet.apache.org/), or pure Python functions.

#### DeltaLake tables
DeltaLake tables provide efficient access to DeltaLake tables:

```python
from neuralake.core.tables import DeltalakeTable
import pyarrow as pa

# Define the schema
schema = pa.schema([
    ("timestamp", pa.timestamp("ns")),
    ("value", pa.float64()),
])

# Create the table
spikes = DeltalakeTable(
    name="spikes",
    uri="s3://my-bucket/spikes",
    schema=schema,
    description="Neural spike data",
    unique_columns=["timestamp"],
)
```

#### Parquet tables
Allow for querying parquet data:

```python
from neuralake.core.tables import ParquetTable, Partition, PartitioningScheme

# Define partitions
partitions = [
    Partition("date", str),
    Partition("subject_id", str),
]

# Create the table
spikes = ParquetTable(
    name="spikes",
    uri="s3://my-bucket/spikes",
    partitioning=partitions,
    partitioning_scheme=PartitioningScheme.HIVE,
    description="Neural spike data",
)
```

#### Function tables
Function tables are created using the `@table` decorator and allow you to define custom data access logic:

```python
from neuralake.core.tables import table
import polars as pl

@table
def my_custom_table() -> NlkDataFrame:
    """A custom table that returns data from any source."""
    # Create a sample dataframe with neural spike data
    data = {
        "timestamp": ["2024-01-01", "2024-01-01", "2024-01-01", "2024-01-01", "2024-01-01"],
        "value": [0.123, 0.456, 0.789, 0.321, 0.654],
        "channel": [1, 2, 3, 4, 5]
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
def spikes():
    """Neural spike data."""
    return NlkDataFrame(...)

@table
def events():
    """Behavioral events."""
    return NlkDataFrame(...)

# Using the database
from neuralake.core.catalog import ModuleDatabase
import my_database

db = ModuleDatabase(my_database)

# Query data
>>> df = db.spikes()
>>> df.head()
shape: (5, 3)
┌────────────┬─────────┬─────────┐
│ timestamp  │ value   │ channel │
├────────────┼─────────┼─────────┤
│ 2024-01-01 │ 0.123   │ 1       │
│ 2024-01-01 │ 0.456   │ 2       │
│ 2024-01-01 │ 0.789   │ 3       │
│ 2024-01-01 │ 0.321   │ 4       │
│ 2024-01-01 │ 0.654   │ 5       │
└────────────┴─────────┴─────────┘
```

#### Custom database
You can also create custom databases by implementing the `Database` protocol:

```python
from neuralake.core.catalog import Database, TableProtocol

class MyDatabase(Database):
    def __init__(self):
        self._tables = {
            "spikes": DeltalakeTable(...),
            "events": ParquetTable(...),
        }

    def get_tables(self, show_deprecated: bool = False) -> dict[str, TableProtocol]:
        return self._tables

    def table(self, name: str, *args, **kwargs) -> NlkDataFrame:
        return self._tables[name](*args, **kwargs)
```

### Catalogs

A catalog in Neuralake is a Python module that is a collection of databases. It provides a unified interface to access data across multiple databases:

```python
from neuralake.core.catalog import Catalog, ModuleDatabase
import neural_spikes
import behavioral_data

# Create a catalog
dbs = {
    "neural_spikes": ModuleDatabase(neural_spikes),
    "behavioral_data": ModuleDatabase(behavioral_data),
}

MyCatalog = Catalog(dbs)

# Query data across databases
>>> spikes = MyCatalog.db("neural_spikes").spikes()
>>> events = MyCatalog.db("behavioral_data").events()

# Join data across databases
>>> joined = spikes.join(events, on="timestamp")
>>> joined.head()
shape: (5, 5)
┌────────────┬─────────┬─────────┬─────────┬────────────┐
│ timestamp  │ value   │ channel │ event   │ event_time │
├────────────┼─────────┼─────────┼─────────┼────────────┤
│ 2024-01-01 │ 0.123   │ 1       │ start   │ 2024-01-01 │
│ 2024-01-01 │ 0.456   │ 2       │ start   │ 2024-01-01 │
│ 2024-01-01 │ 0.789   │ 3       │ stop    │ 2024-01-01 │
│ 2024-01-01 │ 0.321   │ 4       │ stop    │ 2024-01-01 │
│ 2024-01-01 │ 0.654   │ 5       │ start   │ 2024-01-01 │
└────────────┴─────────┴─────────┴─────────┴────────────┘
```

## Querying data

Neuralake provides a consistent interface for querying data across all table types:

```python
# Basic query
>>> df = db.spikes()

# Filter data
>>> df = db.spikes(filters=[("channel", "=", 1)])

# Select columns
>>> df = db.spikes(columns=["timestamp", "value"])

# Complex queries
>>> df = db.spikes(
...     filters=[
...         ("channel", "=", 1),
...         ("timestamp", ">=", "2024-01-01"),
...     ],
...     columns=["timestamp", "value"],
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
>>> df = db.spikes(cache_options=cache_options)
```

### Custom columns
You can add custom computed columns to tables:

```python
# Add a custom column
spikes = DeltalakeTable(
    name="spikes",
    uri="s3://my-bucket/spikes",
    schema=schema,
    extra_cols=[
        (pl.col("value") * 1000, "value_mv"),
    ],
)

# Query with custom column
>>> df = spikes(columns=["timestamp", "value_mv"])
```