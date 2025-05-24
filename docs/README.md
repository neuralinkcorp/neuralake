<!-- Using CSS to hide this on the site, as the logo is already on the nav.-->
<div align="center" class="github-only">
    <img src="images/banner_black.png">
    <br>
    <a href="https://redesigned-adventure-e22eewy.pages.github.io/">
        <img src="https://img.shields.io/badge/DOCS-blue?style=for-the-badge" alt="Documentation">
    </a>
    <a href="https://test.pypi.org/project/neuralake/">
        <img src="https://img.shields.io/badge/PyPI%20%7C%20v0.0.16-blue?style=for-the-badge" alt="PyPI Version">
    </a>
</div>

# Neuralake: A simple platform for complex data

Neuralake is a simple query interface for multimodal data at any scale. 

With Neuralake, you can define a catalog, databases, and tables to query any existing data source. Once you've defined your catalog, you can spin up a static site for easy browsing or a read-only API for programmatic access. No running servers or services!

The Neuralake client has native, declarative connectors to [Delta Lake](https://delta.io/) and [Parquet](https://parquet.apache.org/) stores. Neuralake also supports defining tables via custom Python functions, so you can connect to any data source!

Here's an example catalog:

<div class="github-only">
    <img src="images/catalog.png" />
</div>

<!-- The below comment is replaced by a mkdown hook to insert an iFrame catalog -->
<!-- this is done via hooks because we can't show the iFrame on GitHub, but want to show it on the static site. -->
<!-- mkdocs:iframe -->

## Key features

- **Unified interface**: Query data across different storage modalities (Parquet, DeltaLake, relational databases)
- **Declarative catalog syntax**: Define catalogs in python without running services
- **Catalog site generation**: Generate a static site catalog for visual browsing
- **Extensible**: Declare tables as custom python functions for querying **any** data
- **API support**: Generate a YAML config for querying with [ROAPI](https://github.com/roapi/roapi)
- **Fast**: Uses Rust-native libraries such as [polars](https://github.com/pola-rs/), [delta-rs](https://github.com/delta-io/delta-rs), and [Apache DataFusion](https://github.com/apache/datafusion) for performant reads

## Philosophy
Data engineering should be simple. That means:

1. **Scale up and scale down** - tools should scale down to a developer's laptop and up to stateless clusters
2. **Prioritize local development experience** - use composable libraries instead of distributed services
3. **Code as a catalog** - define tables *in code*, generate a static site catalog and APIs without running services

## Quick start

Install the latest version with:

```bash
pip install neuralake
```

### Create a table and catalog

```python
from neuralake.core import (
    DeltalakeTable,
    ParquetTable,
    Filter,
    Catalog
)

# Define tables
customers = ParquetTable(
    name="customer",
    uri='s3://my-bucket/tpc-h/customer',
    schema=pa.schema([
        ("c_custkey", pa.int64()),
        ("c_name", pa.string()),
        ("c_address", pa.string()),
        ("c_nationkey", pa.int64()),
        ("c_phone", pa.string()),
        ("c_acctbal", pa.decimal128(12, 2)),
        ("c_mktsegment", pa.string()),
        ("c_comment", pa.string()),
    ]),
    docs_filters=[
        Filter("c_custkey", "=", 1),
        Filter("c_mktsegment", "=", "BUILDING"),
    ],
    unique_columns=['c_custkey'],
    description="""
    Customer information from the TPC-H benchmark.
    Contains customer details including name, address, and market segment.
    """,
)

orders = DeltalakeTable(
    name="orders",
    uri='s3://my-bucket/tpc-h/orders',
    schema=pa.schema([
        ("o_orderkey", pa.int64()),
        ("o_custkey", pa.int64()),
        ("o_orderstatus", pa.string()),
        ("o_totalprice", pa.decimal128(12, 2)),
        ("o_orderdate", pa.date32()),
        ("o_orderpriority", pa.string()),
        ("o_clerk", pa.string()),
        ("o_shippriority", pa.int32()),
        ("o_comment", pa.string()),
    ]),
    docs_filters=[
        Filter("o_custkey", "=", 1),
        Filter("o_orderdate", "=", "2024-01-01"),
    ],
    unique_columns=['o_orderkey'],
    description="""
    Order information from the TPC-H benchmark.
    Contains order details including status, total price, and order date.
    """,
)

# Create a catalog
dbs = {
    "tpc-h": {
        "customer": customers,
        "orders": orders,
    }
}

MyCatalog = Catalog(dbs)
```

### Query the data

```python
>>> from my_catalogs import MyCatalog
>>> from neuralake.core import Filter
>>> 
>>> # Get customer information
>>> customer_data = MyCatalog.db("tpc-h").table(
...     "customer",
...     (
...         Filter('c_custkey', '=', 1),
...         Filter('c_mktsegment', '=', 'BUILDING'),
...     ),
... ).collect()
>>> 
>>> # Get their orders
>>> order_data = MyCatalog.db("tpc-h").table(
...     "orders",
...     (
...         Filter('o_custkey', '=', 1),
...         Filter('o_orderdate', '>=', '2024-01-01'),
...     ),
... ).collect()
>>> 
>>> print(customer_data)
shape: (1, 8)
┌───────────┬────────────┬────────────┬────────────┬───────────┬────────────┬────────────┬────────────┐
│ c_custkey │ c_name     │ c_address  │ c_nationkey│ c_phone   │ c_acctbal  │ c_mktsegment│ c_comment │
│ ---       │ ---        │ ---        │ ---        │ ---       │ ---        │ ---        │ ---        │
│ i64       │ str        │ str        │ i64        │ str       │ dec        │ str        │ str        │
╞═══════════╪════════════╪════════════╪════════════╪═══════════╪════════════╪════════════╪════════════╡
│ 1         │ Customer#1 │ Address 1  │ 1          │ 123-456   │ 1000.00    │ BUILDING   │ Comment 1  |
└───────────┴────────────┴────────────┴────────────┴───────────┴────────────┴────────────┴────────────┘

>>> print(order_data)
shape: (3, 9)
┌───────────┬───────────┬──────────────┬─────────────┬────────────┬─────────────────┬───────────┬─────────────────┬───────────┐
│ o_orderkey│ o_custkey │ o_orderstatus│ o_totalprice│ o_orderdate│ o_orderpriority │ o_clerk   │ o_shippriority  │ o_comment │
│ ---       │ ---       │ ---          │ ---         │ ---        │ ---             │ ---       │ ---             │ ---       │
│ i64       │ i64       │ str          │ dec         │ date       │ str             │ str       │ i32             │ str       │
╞═══════════╪═══════════╪══════════════╪═════════════╪════════════╪═════════════════╪═══════════╪═════════════════╪═══════════╡
│ 1         │ …         │ …            │ 1000.00     │ 2024-01-01 │ 5-LOW           │ Clerk#1   │ 0               │ Order 1   │
│ 2         │ …         │ …            │ 2000.00     │ 2024-01-02 │ 4-NOT SPEC      │ Clerk#2   │ 0               │ Order 2   │
│ 3         │ …         │ …            │ 3000.00     │ 2024-01-03 │ 3-MEDIUM        │ Clerk#3   │ 0               │ Order 3   │
└───────────┴───────────┴──────────────┴─────────────┴────────────┴─────────────────┴───────────┴─────────────────┴───────────┘
```

## About Neuralink

Neuralake is part of Neuralink's commitment to the open source community. By maintaining free and open source software, we aim to accelerate data engineering and biotechnology. 

Neuralink is creating a generalized brain interface to restore autonomy to those with unmet medical needs today, and to unlock human potential tomorrow.

You don't have to be a brain surgeon to work at Neuralink. We are looking for exceptional individuals from many fields, including software and data engineering. Learn more at [neuralink.com/careers](https://neuralink.com/careers/).
