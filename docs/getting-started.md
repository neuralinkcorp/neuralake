# Getting Started

## Installation

Install the latest version of Neuralake using pip:

```bash
pip install neuralake
```

## Basic Usage

### Creating a Table

Neuralake provides several table types for different data sources. Here's how to create a Delta Lake table:

```python
from neuralake.core import DeltalakeTable, Filter

# Define a schema
schema = {
    "implant_id": "i64",
    "date": "str",
    "hour": "i64",
    "spike_count": "i64"
}

# Create a table
neural_spikes = DeltalakeTable(
    name="neural_spikes",
    uri='s3://my-bucket/neural_spikes',
    schema=schema,
    docs_filters=[
        Filter("implant_id", "=", 3770),
        Filter("date", "=", "2024-08-28"),
    ],
    unique_columns=['implant_id'],
    description="Neural spikes recorded by a Neuralink implant."
)
```

### Creating a Catalog

A catalog is a collection of tables that can be queried together:

```python
from neuralake.core import Catalog

# Create a catalog
dbs = {
    "neural_spikes": neural_spikes,
}

MyCatalog = Catalog(dbs)
```

### Querying Data

Once you have a catalog, you can query the data using filters:

```python
from neuralake.core import Filter

# Query the data
data = MyCatalog.db("neural_spikes").table(
    "neural_spikes",
    (
        Filter('implant_id', '=', 5555),
        Filter('date', '=', '2024-09-06'),
    ),
).collect()

print(data)
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