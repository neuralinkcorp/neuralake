import sys
from typing import Any

from neuralake.core import DeltalakeTable, ParquetTable
from neuralake.core.catalog.catalog import Catalog
from neuralake.core.tables.metadata import RoapiOptions, TableProtocol
from neuralake.core.tables.util import (
    DeltaRoapiOptions,
    Filter,
    PartitioningScheme,
)


def export_to_roapi_table(name: str, table: TableProtocol) -> dict[str, Any] | None:
    if isinstance(table, ParquetTable):
        return _export_parquet_table(name, table)
    elif isinstance(table, DeltalakeTable):
        return _export_deltalake_table(name, table)
    else:
        print(
            f"{name}: Only parquet and deltalake tables are supported for roapi export.",
            file=sys.stderr,
        )
        return None


def export_to_roapi_tables(catalog: Catalog) -> list[dict[str, Any]]:
    """
    Exports a list of roapi tables that can be injected into a roapi
    configuration yaml under the `tables` key.
    """
    roapi_tables = []
    for db_name in catalog.dbs():
        db = catalog.db(db_name)
        tables = db.get_tables(show_deprecated=True)

        for tbl_name, table in tables.items():
            roapi_table_name = f"{db_name}_{getattr(table, 'name', tbl_name)}"
            if (
                roapi_table := export_to_roapi_table(roapi_table_name, table)
            ) is not None:
                roapi_tables.append(roapi_table)

    return roapi_tables


def _export_parquet_table(name: str, table: ParquetTable) -> dict[str, Any] | None:
    roapi_opts = table.table_metadata.roapi_opts or RoapiOptions()

    if roapi_opts.disable:
        return None

    if table.partitioning_scheme != PartitioningScheme.HIVE:
        print(
            f"{table.name}: Only hive-partitioned parquet tables are supported by roapi.",
            file=sys.stderr,
        )
        return None

    table_info = table.table_metadata

    filters = table_info.docs_args.get("filters", [])
    if not filters:
        return {
            "name": roapi_opts.override_name or name,
            "uri": table.uri,
            "option": {
                "format": "parquet",
                "use_memory_table": roapi_opts.use_memory_table,
            },
        }

    elif not (filters and all(isinstance(x, Filter) for x in filters)):
        print(
            f"{table.name}: Must have docs filters defined to infer partition columns.",
            file=sys.stderr,
        )
        return None

    partition_cols = []
    for filter in filters:
        filter_name = filter.column
        data_type = py_type_to_roapi(type(filter.value))

        # Roapi can treat YYYY-MM-DD partition columns as "Date32"
        data_type = "Date32" if filter_name == "date" else data_type

        partition_cols.append(
            {
                "name": filter_name,
                "data_type": data_type,
            }
        )

    schema_from_file = table.build_file_fragment(filters)

    table_config = {
        "name": roapi_opts.override_name or name,
        "uri": table.uri,
        "option": {
            "format": "parquet",
            "use_memory_table": roapi_opts.use_memory_table,
        },
        "partition_columns": partition_cols,
        "schema_from_files": [schema_from_file],
    }

    return _with_reload_interval(table_config, roapi_opts)


def _export_deltalake_table(name: str, table: DeltalakeTable) -> dict[str, Any] | None:
    roapi_opts = table.table_metadata.roapi_opts or DeltaRoapiOptions()

    if roapi_opts.disable:
        return None

    table_config = {
        "name": roapi_opts.override_name or name,
        "uri": table.uri,
        "option": {
            "format": "delta",
            "use_memory_table": roapi_opts.use_memory_table,
        },
    }

    return _with_reload_interval(table_config, roapi_opts)


def _with_reload_interval(
    table_config: dict[str, Any], roapi_opts: RoapiOptions
) -> dict[str, Any]:
    if roapi_opts.reload_interval_seconds is not None:
        table_config["reload_interval"] = {
            "secs": roapi_opts.reload_interval_seconds,
            "nanos": 0,
        }
    return table_config


def py_type_to_roapi(py_type: type) -> str:
    return {
        int: "Int64",
        str: "Utf8",
        bool: "Boolean",
        float: "Float64",
    }[py_type]
