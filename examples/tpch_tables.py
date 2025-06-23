from neuralake.core import (
    DeltalakeTable,
    ParquetTable,
    Filter,
    table,
    NlkDataFrame,
    Partition,
    PartitioningScheme,
)
import pyarrow as pa
import polars as pl

# Define tables
part = DeltalakeTable(
    name="part",
    uri="s3://my-bucket/tpc-h/part",
    schema=pa.schema(
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
    ),
    docs_filters=[
        Filter("p_partkey", "=", 1),
        Filter("p_brand", "=", "Brand#1"),
    ],
    unique_columns=["p_partkey"],
    description="""
    Part information from the TPC-H benchmark.
    Contains details about parts including name, manufacturer, brand, and retail price.
    """,
    table_metadata_args={
        "data_input": "Part catalog data from manufacturing systems, updated daily",
        "latency_info": "Daily batch updates from manufacturing ERP system",
        "example_notebook": "https://example.com/notebooks/part_analysis.ipynb",
    },
)


@table(
    data_input="Supplier master data from vendor management system <code>/api/suppliers/master</code> endpoint",
    latency_info="Updated weekly by the supplier_master_sync DAG on Airflow",
)
def supplier() -> NlkDataFrame:
    """Supplier information from the TPC-H benchmark."""
    data = {
        "s_suppkey": [1, 2, 3, 4, 5],
        "s_name": [
            "Supplier#1",
            "Supplier#2",
            "Supplier#3",
            "Supplier#4",
            "Supplier#5",
        ],
        "s_address": [
            "123 Main St",
            "456 Oak Ave",
            "789 Pine Rd",
            "321 Elm St",
            "654 Maple Dr",
        ],
        "s_nationkey": [1, 1, 2, 2, 3],
        "s_phone": ["555-0001", "555-0002", "555-0003", "555-0004", "555-0005"],
        "s_acctbal": [1000.00, 2000.00, 3000.00, 4000.00, 5000.00],
        "s_comment": ["Comment 1", "Comment 2", "Comment 3", "Comment 4", "Comment 5"],
    }
    return NlkDataFrame(data)


partsupp = ParquetTable(
    name="partsupp",
    uri="s3://my-bucket/tpc-h/partsupp",
    partitioning=[
        Partition(column="ps_partkey", col_type=pl.Int64),
        Partition(column="ps_suppkey", col_type=pl.Int64),
    ],
    partitioning_scheme=PartitioningScheme.HIVE,
    docs_filters=[
        Filter("ps_partkey", "=", 1),
        Filter("ps_suppkey", "=", 1),
    ],
    description="""
    Part supplier information from the TPC-H benchmark.
    Contains details about parts supplied by suppliers including available quantity and supply cost.
    """,
    table_metadata_args={
        "data_input": "Supplier inventory and pricing data from procurement systems",
        "latency_info": "Real-time updates from supplier inventory management systems",
        "example_notebook": "https://example.com/notebooks/supplier_analysis.ipynb",
    },
)


@table(
    data_input="Customer profile data from CRM system <code>/api/customers/profiles</code> endpoint",
    latency_info="Updated daily by the customer_profile_sync DAG on Airflow",
)
def customer() -> NlkDataFrame:
    """Customer information from the TPC-H benchmark."""
    data = {
        "c_custkey": [1, 2, 3, 4, 5],
        "c_name": [
            "Customer#1",
            "Customer#2",
            "Customer#3",
            "Customer#4",
            "Customer#5",
        ],
        "c_address": [
            "123 Main St",
            "456 Oak Ave",
            "789 Pine Rd",
            "321 Elm St",
            "654 Maple Dr",
        ],
        "c_nationkey": [1, 1, 2, 2, 3],
        "c_phone": ["555-0001", "555-0002", "555-0003", "555-0004", "555-0005"],
        "c_acctbal": [1000.00, 2000.00, 3000.00, 4000.00, 5000.00],
        "c_mktsegment": [
            "BUILDING",
            "AUTOMOBILE",
            "MACHINERY",
            "HOUSEHOLD",
            "FURNITURE",
        ],
        "c_comment": ["Comment 1", "Comment 2", "Comment 3", "Comment 4", "Comment 5"],
    }
    return NlkDataFrame(data)


orders = DeltalakeTable(
    name="orders",
    uri="s3://my-bucket/tpc-h/orders",
    schema=pa.schema(
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
    ),
    docs_filters=[
        Filter("o_custkey", "=", 1),
        Filter("o_orderdate", "=", "2024-01-01"),
    ],
    unique_columns=["o_orderkey"],
    description="""
    Order information from the TPC-H benchmark.
    Contains order details including status, total price, and order date.
    """,
    table_metadata_args={
        "data_input": "Order data from e-commerce and retail systems",
        "latency_info": "Near real-time order processing with 5-minute delay",
        "example_notebook": "https://example.com/notebooks/order_analysis.ipynb",
    },
)


@table(
    data_input="Order line items from fulfillment system <code>/api/orders/line_items</code> endpoint",
    latency_info="Updated hourly by the order_line_sync DAG on Airflow",
)
def lineitem() -> NlkDataFrame:
    """Line item information from the TPC-H benchmark."""
    data = {
        "l_orderkey": [1, 1, 2, 2, 3],
        "l_partkey": [1, 2, 3, 4, 5],
        "l_suppkey": [1, 2, 3, 4, 5],
        "l_linenumber": [1, 2, 1, 2, 1],
        "l_quantity": [10.0, 20.0, 30.0, 40.0, 50.0],
        "l_extendedprice": [1000.00, 2000.00, 3000.00, 4000.00, 5000.00],
        "l_discount": [0.1, 0.2, 0.3, 0.4, 0.5],
        "l_tax": [0.05, 0.06, 0.07, 0.08, 0.09],
        "l_returnflag": ["N", "N", "R", "R", "A"],
        "l_linestatus": ["O", "O", "F", "F", "F"],
        "l_shipdate": [
            "2024-01-01",
            "2024-01-02",
            "2024-01-03",
            "2024-01-04",
            "2024-01-05",
        ],
        "l_commitdate": [
            "2024-01-02",
            "2024-01-03",
            "2024-01-04",
            "2024-01-05",
            "2024-01-06",
        ],
        "l_receiptdate": [
            "2024-01-03",
            "2024-01-04",
            "2024-01-05",
            "2024-01-06",
            "2024-01-07",
        ],
        "l_shipinstruct": [
            "DELIVER IN PERSON",
            "COLLECT COD",
            "TAKE BACK RETURN",
            "NONE",
            "DELIVER IN PERSON",
        ],
        "l_shipmode": ["TRUCK", "SHIP", "AIR", "RAIL", "TRUCK"],
        "l_comment": ["Comment 1", "Comment 2", "Comment 3", "Comment 4", "Comment 5"],
    }
    return NlkDataFrame(data)


nation = ParquetTable(
    name="nation",
    uri="s3://my-bucket/tpc-h/nation",
    partitioning=[
        Partition(column="n_nationkey", col_type=pl.Int64),
        Partition(column="n_regionkey", col_type=pl.Int64),
    ],
    partitioning_scheme=PartitioningScheme.HIVE,
    docs_filters=[
        Filter("n_nationkey", "=", 1),
        Filter("n_regionkey", "=", 1),
    ],
    description="""
    Nation information from the TPC-H benchmark.
    Contains details about nations including name and region.
    """,
    table_metadata_args={
        "data_input": "Geographic reference data from global operations database",
        "latency_info": "Static reference data, updated quarterly",
        "example_notebook": "https://example.com/notebooks/geographic_analysis.ipynb",
    },
)


@table(
    data_input="Regional hierarchy data from global operations <code>/api/regions/hierarchy</code> endpoint",
    latency_info="Updated monthly by the region_hierarchy_sync DAG on Airflow",
)
def region() -> NlkDataFrame:
    """Region information from the TPC-H benchmark."""
    data = {
        "r_regionkey": [1, 2, 3, 4, 5],
        "r_name": ["AMERICA", "ASIA", "EUROPE", "AFRICA", "MIDDLE EAST"],
        "r_comment": ["Comment 1", "Comment 2", "Comment 3", "Comment 4", "Comment 5"],
    }
    return NlkDataFrame(data)
