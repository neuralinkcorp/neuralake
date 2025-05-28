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

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stderr)
logger = logging.getLogger(__name__)

def check_node_dependencies():
    """Check if NodeJS is installed."""
    # Check for NodeJS
    try:
        node = subprocess.run(["node", "--version"], capture_output=True, check=True)
        logger.info(f"Found NodeJS {node.stdout.decode('utf-8').strip()}")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        logger.warning("\nWARNING: NodeJS is missing, command node --version failed.")
        logger.warning("\nNodeJS is required for generating the catalog static site.")
        logger.warning("To install NodeJS:")
        logger.warning("  1. Visit https://nodejs.org/ to download and install NodeJS")
        return False


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
    Export the catalog to a JSON file and generate a static site using web_catalog/cli.js.

    Args:
        catalogs: List of (name, catalog) tuples to export
        output_dir: Directory where the static site will be generated (default: "./dist")
    """
    # Check for NodeJS dependencies first
    if not check_node_dependencies():
        raise RuntimeError("Required NodeJS dependencies are missing. Please install NodeJS to generate static files.")

    # Export catalog to a JSON file
    catalog_data = export_neuralake(catalogs)

    with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as f:
        json.dump(catalog_data, f, indent=2)
        catalog_file = Path(f.name)

    try:
        project_root = Path(__file__).parent
        web_catalog_dir = project_root / "export" / "static_site"
        cli_path = web_catalog_dir / "cli.js"

        if not cli_path.exists():
            raise FileNotFoundError(
                f"Could not find cli.js at {cli_path}. Make sure you're running from the project root."
            )

        try:
            os.chdir(str(web_catalog_dir))

            # TODO: Remove --legacy-peer-deps
            subprocess.run(["npm", "ci", "--legacy-peer-deps"], check=True)

            subprocess.run(
                [
                    "node",
                    str(cli_path),
                    "-f",
                    str(catalog_file.absolute()),
                    "-o",
                    output_dir,
                ],
                check=True,
            )
        finally:
            os.chdir(str(project_root))
    finally:
        if catalog_file.exists():
            catalog_file.unlink()
