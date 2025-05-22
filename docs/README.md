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

<div align="center">
    <img src="images/catalog.png" alt="Neuralake logo">
</div>


Neuralake is a simple query interface for multimodal data at any scale. 

With Neuralake, you can define a catalog, databases, and tables to query any existing data source. Once you've defined your catalog, you can spin up a static site for easy browsing or a read-only API for programmatic access. No running servers or services!

The Neuralake client has native, declarative connectors to [Delta Lake](https://delta.io/) and [Parquet](https://parquet.apache.org/) stores. Neuralake also supports defining tables via custom Python functions, so you can connect to any data source!

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
    Filter,
    Catalog
)

# Define a table
schema = pa.schema([
    ("implant_id", pa.int64()),
    ("date", pa.date64()),
    ("hour", pa.int8()),
])

neural_spikes = DeltalakeTable(
    name="my_table",
    uri='s3://my-bucket/neural_spikes',
    schema=schema,
    # Example filters for auto-generated docs
    docs_filters=[
        Filter("implant_id", "=", 3770),
        Filter("date", "=", "2024-08-28"),
    ],
    unique_columns=['implant_id'],
    description="""
    Neural spikes recorded by a Neuralink implant.
    This data is published every 10 seconds to a Delta Lake table.
    """,
)

# Create a catalog
dbs = {
    "neural_spikes": neural_spikes,
}

MyCatalog = Catalog(dbs)
```

### Query the data

```python
>>> from my_catalogs import MyCatalog
>>> from neuralake.core import Filter
>>> 
>>> data = MyCatalog.db("neural_spikes").table(
...     "my_table",
...     (
...         Filter('implant_id', '=', 5555),
...         Filter('date', '=', '2024-09-06'),
...     ),
... ).collect()
>>> 
>>> print(data)
shape: (3, 50)
┌────────────┬────────────┬──────┐
│ implant_id ┆ date       ┆ hour │
│ ---        ┆ ---        ┆ ---  │
│ i64        ┆ str        ┆ i64  │
╞════════════╪════════════╪══════╡
│ 5555       ┆ 2024-09-06 ┆ 7    │
│ 5555       ┆ 2024-09-06 ┆ 7    │
│ …          ┆ …          ┆ …    │
│ 5555       ┆ 2024-09-06 ┆ 6    │
└────────────┴────────────┴──────┘
```

## About Neuralink

Neuralake is part of Neuralink's commitment to the open source community. By maintaining free and open source software, we aim to accelerate data engineering and biotechnology. 

Neuralink is creating a generalized brain interface to restore autonomy to those with unmet medical needs today, and to unlock human potential tomorrow.

You don't have to be a brain surgeon to work at Neuralink. We are looking for exceptional individuals from many fields, including software and data engineering. Learn more at [neuralink.com/careers](https://neuralink.com/careers/).
