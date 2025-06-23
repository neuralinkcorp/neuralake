#!/usr/bin/env python3

import os
import sys
import argparse
from pathlib import Path
import pyarrow as pa
from unittest.mock import patch

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from neuralake.export.web import export_and_generate_site
from examples.tcph_catalog import TPCHCatalog
from neuralake.core.tables import TableSchema


def mock_get_schema(self):
    """Mock implementation of get_schema that returns a hardcoded schema"""
    # Define the schema based on the table name
    if self.name == "part":
        schema = pa.schema(
            [
                ("p_partkey", pa.int64()),
                ("p_name", pa.string()),
                ("p_mfgr", pa.string()),
                ("p_brand", pa.string()),
                ("p_type", pa.string()),
                ("p_size", pa.int32()),
                ("p_container", pa.string()),
                ("p_retailprice", pa.decimal128(12, 2)),
                ("p_comment", pa.string()),
            ]
        )
        partitions = [
            {"column_name": "p_partkey", "type_annotation": "int", "value": 1}
        ]
    elif self.name == "supplier":
        schema = pa.schema(
            [
                ("s_suppkey", pa.int64()),
                ("s_name", pa.string()),
                ("s_address", pa.string()),
                ("s_nationkey", pa.int64()),
                ("s_phone", pa.string()),
                ("s_acctbal", pa.decimal128(12, 2)),
                ("s_comment", pa.string()),
            ]
        )
        partitions = [
            {"column_name": "s_suppkey", "type_annotation": "int", "value": 1}
        ]
    elif self.name == "partsupp":
        schema = pa.schema(
            [
                ("ps_partkey", pa.int64()),
                ("ps_suppkey", pa.int64()),
                ("ps_availqty", pa.int32()),
                ("ps_supplycost", pa.decimal128(12, 2)),
                ("ps_comment", pa.string()),
            ]
        )
        partitions = [
            {"column_name": "ps_partkey", "type_annotation": "int", "value": 1},
            {"column_name": "ps_suppkey", "type_annotation": "int", "value": 1},
        ]
    elif self.name == "customer":
        schema = pa.schema(
            [
                ("c_custkey", pa.int64()),
                ("c_name", pa.string()),
                ("c_address", pa.string()),
                ("c_nationkey", pa.int64()),
                ("c_phone", pa.string()),
                ("c_acctbal", pa.decimal128(12, 2)),
                ("c_mktsegment", pa.string()),
                ("c_comment", pa.string()),
            ]
        )
        partitions = [
            {"column_name": "c_custkey", "type_annotation": "int", "value": 1}
        ]
    elif self.name == "orders":
        schema = pa.schema(
            [
                ("o_orderkey", pa.int64()),
                ("o_custkey", pa.int64()),
                ("o_orderstatus", pa.string()),
                ("o_totalprice", pa.decimal128(12, 2)),
                ("o_orderdate", pa.date32()),
                ("o_orderpriority", pa.string()),
                ("o_clerk", pa.string()),
                ("o_shippriority", pa.int32()),
                ("o_comment", pa.string()),
            ]
        )
        partitions = [
            {"column_name": "o_orderkey", "type_annotation": "int", "value": 1}
        ]
    elif self.name == "lineitem":
        schema = pa.schema(
            [
                ("l_orderkey", pa.int64()),
                ("l_partkey", pa.int64()),
                ("l_suppkey", pa.int64()),
                ("l_linenumber", pa.int32()),
                ("l_quantity", pa.decimal128(12, 2)),
                ("l_extendedprice", pa.decimal128(12, 2)),
                ("l_discount", pa.decimal128(12, 2)),
                ("l_tax", pa.decimal128(12, 2)),
                ("l_returnflag", pa.string()),
                ("l_linestatus", pa.string()),
                ("l_shipdate", pa.date32()),
                ("l_commitdate", pa.date32()),
                ("l_receiptdate", pa.date32()),
                ("l_shipinstruct", pa.string()),
                ("l_shipmode", pa.string()),
                ("l_comment", pa.string()),
            ]
        )
        partitions = [
            {"column_name": "l_orderkey", "type_annotation": "int", "value": 1},
            {"column_name": "l_linenumber", "type_annotation": "int", "value": 1},
        ]
    elif self.name == "nation":
        schema = pa.schema(
            [
                ("n_nationkey", pa.int64()),
                ("n_name", pa.string()),
                ("n_regionkey", pa.int64()),
                ("n_comment", pa.string()),
            ]
        )
        partitions = [
            {"column_name": "n_nationkey", "type_annotation": "int", "value": 1}
        ]
    elif self.name == "region":
        schema = pa.schema(
            [
                ("r_regionkey", pa.int64()),
                ("r_name", pa.string()),
                ("r_comment", pa.string()),
            ]
        )
        partitions = [
            {"column_name": "r_regionkey", "type_annotation": "int", "value": 1}
        ]
    else:
        raise ValueError(f"Unknown table name: {self.name}")

    columns = [
        {
            "name": name,
            "type": str(field.type),
        }
        for name, field in zip(schema.names, schema)
    ]

    return TableSchema(partitions=partitions, columns=columns)


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Generate TPC-H web catalog")
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory for the web catalog (default: ./web_catalog)",
    )
    args = parser.parse_args()

    # Get the output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = Path(__file__).parent / "web_catalog"
    output_dir = output_dir.resolve()

    # Create the output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    # Mock the get_schema method for both ParquetTable and DeltalakeTable
    with patch(
        "neuralake.core.tables.parquet_table.ParquetTable.get_schema", mock_get_schema
    ), patch(
        "neuralake.core.tables.deltalake_table.DeltalakeTable.get_schema",
        mock_get_schema,
    ):
        # Export and generate the site
        export_and_generate_site(
            catalogs=[("tcph", TPCHCatalog)], output_dir=str(output_dir)
        )

    print(f"Static site generated at: {output_dir}")


if __name__ == "__main__":
    main()
