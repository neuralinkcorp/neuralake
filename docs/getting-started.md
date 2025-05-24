# Getting Started

## Installation

Install the latest version of Neuralake using pip:

```bash
pip install neuralake
```

## Basic Usage

### Creating a Table

Neuralake provides several table types for different data sources. Here's how to create Delta Lake and Parquet tables:

```python
from neuralake.core import DeltalakeTable, ParquetTable, Filter
import pyarrow as pa

# Define schemas
supplier_schema = pa.schema([
    ("s_suppkey", pa.int64()),
    ("s_name", pa.string()),
    ("s_address", pa.string()),
    ("s_nationkey", pa.int64()),
    ("s_phone", pa.string()),
    ("s_acctbal", pa.decimal128(12, 2)),
    ("s_comment", pa.string()),
])

partsupp_schema = pa.schema([
    ("ps_partkey", pa.int64()),
    ("ps_suppkey", pa.int64()),
    ("ps_availqty", pa.int32()),
    ("ps_supplycost", pa.decimal128(12, 2)),
    ("ps_comment", pa.string()),
])

# Create a Delta Lake table
supplier = DeltalakeTable(
    name="supplier",
    uri='s3://my-bucket/tpc-h/supplier',
    schema=supplier_schema,
    docs_filters=[
        Filter("s_suppkey", "=", 1),
        Filter("s_nationkey", "=", 1),
    ],
    unique_columns=['s_suppkey'],
    description="Supplier information from the TPC-H benchmark."
)

# Create a Parquet table
partsupp = ParquetTable(
    name="partsupp",
    uri='s3://my-bucket/tpc-h/partsupp',
    schema=partsupp_schema,
    docs_filters=[
        Filter("ps_suppkey", "=", 1),
        Filter("ps_partkey", "=", 1),
    ],
    unique_columns=['ps_partkey', 'ps_suppkey'],
    description="Part supplier relationship information from the TPC-H benchmark."
)
```

### Creating a Catalog

A catalog is a collection of tables:

```python
from neuralake.core import Catalog

# Create a catalog
dbs = {
    "tpc-h": {
        "supplier": supplier,
        "partsupp": partsupp,
    }
}

MyCatalog = Catalog(dbs)
```

### Querying Data

```python
from neuralake.core import Filter
import polars as pl

# Query and join tables
joined_data = (
    MyCatalog.db("tpc-h").table("supplier", Filter('s_suppkey', '=', 1))
    .join(
        MyCatalog.db("tpc-h").table("partsupp", Filter('ps_suppkey', '=', 1)),
        left_on="s_suppkey",
        right_on="ps_suppkey",
        how="inner"
    )
    .select(["s_suppkey", "s_name", "ps_partkey", "ps_availqty"])
    .collect()
)

print(joined_data)
```

### Generating Documentation

Neuralake can generate documentation for your catalog:

```python
from neuralake import web_export

# Generate documentation
web_export.generate_docs(MyCatalog, output_dir="docs")
```

### API Configuration

You can also generate a YAML configuration for [ROAPI](https://github.com/roapi/roapi):

```python
from neuralake import roapi_export

# Generate ROAPI config
roapi_export.generate_config(MyCatalog, output_file="roapi-config.yaml")
```