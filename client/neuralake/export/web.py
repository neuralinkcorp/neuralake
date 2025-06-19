from neuralake.core.catalog.catalog import Catalog, Database
from neuralake.core.tables.deltalake_table import DeltalakeTable
from neuralake.core.tables.metadata import TableProtocol
import json
import tempfile
import subprocess
from pathlib import Path
import os
import sys
import logging
import shutil
import importlib.resources

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stderr)
logger = logging.getLogger(__name__)


def export_table(name: str, table: TableProtocol):
    table_info = table.table_metadata

    schema = table.get_schema()
    partitions, columns = schema.partitions, schema.columns

    return {
        "name": name,
        "description": table_info.description,
        "partitions": partitions,
        "columns": columns,
        "selected_columns": table_info.docs_args.get("columns", None),
        "supports_sql_filter": isinstance(table, DeltalakeTable),
        "table_type": table_info.table_type,
        "latency_info": table_info.latency_info,
        "example_notebook": table_info.example_notebook,
        "data_input": table_info.data_input,
    }


def export_database(name: str, database: Database):
    tables = []

    for key, table in sorted(database.get_tables().items()):
        tables.append(export_table(key, table))

    return {
        "name": name,
        "tables": tables,
    }


def export_catalog(name: str, catalog: Catalog):
    return {
        "name": name,
        "databases": [export_database(key, catalog.db(key)) for key in catalog.dbs()],
    }


def export_neuralake(catalogs: list[tuple[str, Catalog]]) -> dict:
    return {
        "catalogs": [export_catalog(name, catalog) for name, catalog in catalogs],
    }


def export_and_generate_site(
    catalogs: list[tuple[str, Catalog]], output_dir: str = "./dist"
):
    """
    Export the catalog to a JSON file and copy the precompiled static site files.

    Args:
        catalogs: List of (name, catalog) tuples to export
        output_dir: Directory where the static site will be generated (default: "./dist")
    """
    # Export catalog to a JSON file
    catalog_data = export_neuralake(catalogs)

    output_path = Path(output_dir)
    # Remove output directory if it exists to ensure idempotency
    if output_path.exists():
        shutil.rmtree(output_path)
    output_path.mkdir(parents=True, exist_ok=True)

    with open(output_path / "data.json", "w") as f:
        json.dump(catalog_data, f)

    project_root = Path(__file__).parent
    precompiled_dir = project_root / "export" / "static_site" / "precompiled"

    logger.info(f"Copying precompiled directory {precompiled_dir} to {output_path}")
    if not precompiled_dir.exists():
        raise FileNotFoundError(
            f"Could not find precompiled directory. Make sure you're running from the project root or the package is properly installed."
        )

    shutil.copytree(precompiled_dir, output_path, dirs_exist_ok=True)
