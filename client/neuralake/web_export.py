from neuralake.core.catalog.catalog import Catalog, Database
from neuralake.core.tables.deltalake_table import DeltalakeTable
from neuralake.core.tables.metadata import TableProtocol


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
