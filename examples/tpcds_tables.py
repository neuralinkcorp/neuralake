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
customer_address = DeltalakeTable(
    name="customer_address",
    uri="s3://my-bucket/tpc-ds/customer_address",
    schema=pa.schema(
        [
            ("ca_address_sk", pa.int64()),
            ("ca_address_id", pa.string()),
            ("ca_street_number", pa.string()),
            ("ca_street_name", pa.string()),
            ("ca_street_type", pa.string()),
            ("ca_suite_number", pa.string()),
            ("ca_city", pa.string()),
            ("ca_county", pa.string()),
            ("ca_state", pa.string()),
            ("ca_zip", pa.string()),
            ("ca_country", pa.string()),
            ("ca_gmt_offset", pa.float32()),
            ("ca_location_type", pa.string()),
        ]
    ),
    docs_filters=[
        Filter("ca_address_sk", "=", 1),
        Filter("ca_state", "=", "CA"),
    ],
    unique_columns=["ca_address_sk"],
    description="""
    Customer address information from the TPC-DS benchmark.
    Contains address details including street, city, state, and geographic information.
    """,
    table_metadata_args={
        "data_input": "Customer address data from CRM and billing systems",
        "latency_info": "Daily batch updates from customer management system",
        "example_notebook": "https://example.com/notebooks/customer_address_analysis.ipynb",
    },
)

customer_demographics = DeltalakeTable(
    name="customer_demographics",
    uri="s3://my-bucket/tpc-ds/customer_demographics",
    schema=pa.schema(
        [
            ("cd_demo_sk", pa.int64()),
            ("cd_gender", pa.string()),
            ("cd_marital_status", pa.string()),
            ("cd_education_status", pa.string()),
            ("cd_purchase_estimate", pa.int64()),
            ("cd_credit_rating", pa.string()),
            ("cd_dep_count", pa.int64()),
            ("cd_dep_employed_count", pa.int64()),
            ("cd_dep_college_count", pa.int64()),
        ]
    ),
    docs_filters=[
        Filter("cd_demo_sk", "=", 1),
        Filter("cd_gender", "=", "M"),
    ],
    unique_columns=["cd_demo_sk"],
    description="""
    Customer demographic information from the TPC-DS benchmark.
    Contains demographic details including gender, marital status, education, and family information.
    """,
    table_metadata_args={
        "data_input": "Customer demographic data from survey and registration systems",
        "latency_info": "Weekly updates from customer profiling system",
        "example_notebook": "https://example.com/notebooks/customer_demographics_analysis.ipynb",
    },
)

date_dim = DeltalakeTable(
    name="date_dim",
    uri="s3://my-bucket/tpc-ds/date_dim",
    schema=pa.schema(
        [
            ("d_date_sk", pa.int64()),
            ("d_date_id", pa.string()),
            ("d_date", pa.date32()),
            ("d_month_seq", pa.int64()),
            ("d_week_seq", pa.int64()),
            ("d_quarter_seq", pa.int64()),
            ("d_year", pa.int64()),
            ("d_dow", pa.int64()),
            ("d_moy", pa.int64()),
            ("d_dom", pa.int64()),
            ("d_qoy", pa.int64()),
            ("d_fy_year", pa.int64()),
            ("d_fy_quarter_seq", pa.int64()),
            ("d_fy_week_seq", pa.int64()),
            ("d_day_name", pa.string()),
            ("d_quarter_name", pa.string()),
            ("d_holiday", pa.string()),
            ("d_weekend", pa.string()),
            ("d_following_holiday", pa.string()),
            ("d_first_dom", pa.int64()),
            ("d_last_dom", pa.int64()),
            ("d_same_day_ly", pa.int64()),
            ("d_same_day_lq", pa.int64()),
            ("d_current_day", pa.string()),
            ("d_current_week", pa.string()),
            ("d_current_month", pa.string()),
            ("d_current_quarter", pa.string()),
            ("d_current_year", pa.string()),
        ]
    ),
    docs_filters=[
        Filter("d_date_sk", "=", 1),
        Filter("d_year", "=", 2024),
    ],
    unique_columns=["d_date_sk"],
    description="""
    Date dimension table from the TPC-DS benchmark.
    Contains comprehensive date information including fiscal periods, holidays, and calendar attributes.
    """,
    table_metadata_args={
        "data_input": "Date dimension data from enterprise calendar system",
        "latency_info": "Static reference data, updated annually for new fiscal years",
        "example_notebook": "https://example.com/notebooks/date_dimension_analysis.ipynb",
    },
)

@table(
    data_input="Warehouse master data from logistics system <code>/api/warehouses/master</code> endpoint",
    latency_info="Updated daily by the warehouse_master_sync DAG on Airflow",
)
def warehouse() -> NlkDataFrame:
    """Warehouse information from the TPC-DS benchmark."""
    data = {
        "w_warehouse_sk": [1, 2, 3, 4, 5],
        "w_warehouse_id": [
            "WAREHOUSE_1",
            "WAREHOUSE_2", 
            "WAREHOUSE_3",
            "WAREHOUSE_4",
            "WAREHOUSE_5",
        ],
        "w_warehouse_name": [
            "Central Warehouse",
            "North Distribution Center",
            "South Fulfillment Hub",
            "East Regional Depot",
            "West Coast Facility",
        ],
        "w_warehouse_sq_ft": [100000, 150000, 120000, 80000, 200000],
        "w_street_number": ["123", "456", "789", "321", "654"],
        "w_street_name": ["Main St", "Oak Ave", "Pine Rd", "Elm St", "Maple Dr"],
        "w_street_type": ["Street", "Avenue", "Road", "Street", "Drive"],
        "w_suite_number": [None, "Suite 100", None, "Unit A", "Building B"],
        "w_city": ["Chicago", "Dallas", "Atlanta", "Boston", "Los Angeles"],
        "w_county": ["Cook", "Dallas", "Fulton", "Suffolk", "Los Angeles"],
        "w_state": ["IL", "TX", "GA", "MA", "CA"],
        "w_zip": ["60601", "75201", "30301", "02101", "90210"],
        "w_country": ["United States", "United States", "United States", "United States", "United States"],
        "w_gmt_offset": [-6.0, -6.0, -5.0, -5.0, -8.0],
    }
    return NlkDataFrame(data)

ship_mode = ParquetTable(
    name="ship_mode",
    uri="s3://my-bucket/tpc-ds/ship_mode",
    partitioning=[
        Partition(column="sm_ship_mode_sk", col_type=pl.Int64),
    ],
    partitioning_scheme=PartitioningScheme.HIVE,
    docs_filters=[
        Filter("sm_ship_mode_sk", "=", 1),
        Filter("sm_type", "=", "GROUND"),
    ],
    description="""
    Shipping mode information from the TPC-DS benchmark.
    Contains shipping method details including type, code, carrier, and contract information.
    """,
    table_metadata_args={
        "data_input": "Shipping configuration from logistics management system",
        "latency_info": "Updated weekly by logistics operations team",
        "example_notebook": "https://example.com/notebooks/shipping_analysis.ipynb",
    },
)

time_dim = DeltalakeTable(
    name="time_dim",
    uri="s3://my-bucket/tpc-ds/time_dim",
    schema=pa.schema(
        [
            ("t_time_sk", pa.int64()),
            ("t_time_id", pa.string()),
            ("t_time", pa.int64()),
            ("t_hour", pa.int64()),
            ("t_minute", pa.int64()),
            ("t_second", pa.int64()),
            ("t_am_pm", pa.string()),
            ("t_shift", pa.string()),
            ("t_sub_shift", pa.string()),
            ("t_meal_time", pa.string()),
        ]
    ),
    docs_filters=[
        Filter("t_time_sk", "=", 1),
        Filter("t_hour", "=", 12),
    ],
    unique_columns=["t_time_sk"],
    description="""
    Time dimension table from the TPC-DS benchmark.
    Contains time-of-day information including hours, minutes, shifts, and meal periods.
    """,
    table_metadata_args={
        "data_input": "Time dimension data from enterprise scheduling system",
        "latency_info": "Static reference data, pre-populated for all time periods",
        "example_notebook": "https://example.com/notebooks/time_dimension_analysis.ipynb",
    },
)

@table(
    data_input="Return reason codes from customer service system <code>/api/returns/reasons</code> endpoint",
    latency_info="Updated monthly by the customer_service_sync DAG on Airflow",
)
def reason() -> NlkDataFrame:
    """Return reason information from the TPC-DS benchmark."""
    data = {
        "r_reason_sk": [1, 2, 3, 4, 5],
        "r_reason_id": ["REASON_1", "REASON_2", "REASON_3", "REASON_4", "REASON_5"],
        "r_reason_desc": [
            "Defective product",
            "Wrong item shipped",
            "Customer changed mind",
            "Item damaged in shipping",
            "Item not as described",
        ],
    }
    return NlkDataFrame(data)

@table(
    data_input="Income band definitions from market research system <code>/api/demographics/income_bands</code> endpoint",
    latency_info="Updated annually by the demographics_sync DAG on Airflow",
)
def income_band() -> NlkDataFrame:
    """Income band information from the TPC-DS benchmark."""
    data = {
        "ib_income_band_sk": [1, 2, 3, 4, 5],
        "ib_lower_bound": [0, 15000, 30000, 50000, 75000],
        "ib_upper_bound": [14999, 29999, 49999, 74999, 100000],
    }
    return NlkDataFrame(data)

item = DeltalakeTable(
    name="item",
    uri="s3://my-bucket/tpc-ds/item",
    schema=pa.schema(
        [
            ("i_item_sk", pa.int64()),
            ("i_item_id", pa.string()),
            ("i_rec_start_date", pa.date32()),
            ("i_rec_end_date", pa.date32()),
            ("i_item_desc", pa.string()),
            ("i_current_price", pa.float32()),
            ("i_wholesale_cost", pa.float32()),
            ("i_brand_id", pa.int64()),
            ("i_brand", pa.string()),
            ("i_class_id", pa.int64()),
            ("i_class", pa.string()),
            ("i_category_id", pa.int64()),
            ("i_category", pa.string()),
            ("i_manufact_id", pa.int64()),
            ("i_manufact", pa.string()),
            ("i_size", pa.string()),
            ("i_formulation", pa.string()),
            ("i_color", pa.string()),
            ("i_units", pa.string()),
            ("i_container", pa.string()),
            ("i_manager_id", pa.int64()),
            ("i_product_name", pa.string()),
        ]
    ),
    docs_filters=[
        Filter("i_item_sk", "=", 1),
        Filter("i_category", "=", "Electronics"),
    ],
    unique_columns=["i_item_sk"],
    description="""
    Item information from the TPC-DS benchmark.
    Contains product details including pricing, brand, category, and manufacturing information.
    """,
    table_metadata_args={
        "data_input": "Product catalog data from inventory management system",
        "latency_info": "Daily updates from product information management system",
        "example_notebook": "https://example.com/notebooks/item_analysis.ipynb",
    },
)

store = DeltalakeTable(
    name="store",
    uri="s3://my-bucket/tpc-ds/store",
    schema=pa.schema(
        [
            ("s_store_sk", pa.int64()),
            ("s_store_id", pa.string()),
            ("s_rec_start_date", pa.date32()),
            ("s_rec_end_date", pa.date32()),
            ("s_closed_date_sk", pa.int64()),
            ("s_store_name", pa.string()),
            ("s_number_employees", pa.int64()),
            ("s_floor_space", pa.int64()),
            ("s_hours", pa.string()),
            ("s_manager", pa.string()),
            ("s_market_id", pa.int64()),
            ("s_geography_class", pa.string()),
            ("s_market_desc", pa.string()),
            ("s_market_manager", pa.string()),
            ("s_division_id", pa.int64()),
            ("s_division_name", pa.string()),
            ("s_company_id", pa.int64()),
            ("s_company_name", pa.string()),
            ("s_street_number", pa.string()),
            ("s_street_name", pa.string()),
            ("s_street_type", pa.string()),
            ("s_suite_number", pa.string()),
            ("s_city", pa.string()),
            ("s_county", pa.string()),
            ("s_state", pa.string()),
            ("s_zip", pa.string()),
            ("s_country", pa.string()),
            ("s_gmt_offset", pa.float32()),
            ("s_tax_precentage", pa.float32()),
        ]
    ),
    docs_filters=[
        Filter("s_store_sk", "=", 1),
        Filter("s_state", "=", "CA"),
    ],
    unique_columns=["s_store_sk"],
    description="""
    Store information from the TPC-DS benchmark.
    Contains retail store details including location, management, and operational information.
    """,
    table_metadata_args={
        "data_input": "Store master data from retail operations system",
        "latency_info": "Daily updates from store management system",
        "example_notebook": "https://example.com/notebooks/store_analysis.ipynb",
    },
)

call_center = DeltalakeTable(
    name="call_center",
    uri="s3://my-bucket/tpc-ds/call_center",
    schema=pa.schema(
        [
            ("cc_call_center_sk", pa.int64()),
            ("cc_call_center_id", pa.string()),
            ("cc_rec_start_date", pa.date32()),
            ("cc_rec_end_date", pa.date32()),
            ("cc_closed_date_sk", pa.int64()),
            ("cc_open_date_sk", pa.int64()),
            ("cc_name", pa.string()),
            ("cc_class", pa.string()),
            ("cc_employees", pa.int64()),
            ("cc_sq_ft", pa.int64()),
            ("cc_hours", pa.string()),
            ("cc_manager", pa.string()),
            ("cc_mkt_id", pa.int64()),
            ("cc_mkt_class", pa.string()),
            ("cc_mkt_desc", pa.string()),
            ("cc_market_manager", pa.string()),
            ("cc_division", pa.int64()),
            ("cc_division_name", pa.string()),
            ("cc_company", pa.int64()),
            ("cc_company_name", pa.string()),
            ("cc_street_number", pa.string()),
            ("cc_street_name", pa.string()),
            ("cc_street_type", pa.string()),
            ("cc_suite_number", pa.string()),
            ("cc_city", pa.string()),
            ("cc_county", pa.string()),
            ("cc_state", pa.string()),
            ("cc_zip", pa.string()),
            ("cc_country", pa.string()),
            ("cc_gmt_offset", pa.float32()),
            ("cc_tax_percentage", pa.float32()),
        ]
    ),
    docs_filters=[
        Filter("cc_call_center_sk", "=", 1),
        Filter("cc_state", "=", "CA"),
    ],
    unique_columns=["cc_call_center_sk"],
    description="""
    Call center information from the TPC-DS benchmark.
    Contains call center details including location, management, and operational information.
    """,
    table_metadata_args={
        "data_input": "Call center master data from customer service system",
        "latency_info": "Weekly updates from call center management system",
        "example_notebook": "https://example.com/notebooks/call_center_analysis.ipynb",
    },
)

@table(
    data_input="Customer profile data from CRM system <code>/api/customers/profiles</code> endpoint",
    latency_info="Updated daily by the customer_profile_sync DAG on Airflow",
)
def customer() -> NlkDataFrame:
    """Customer information from the TPC-DS benchmark."""
    data = {
        "c_customer_sk": [1, 2, 3, 4, 5],
        "c_customer_id": [
            "CUSTOMER_1",
            "CUSTOMER_2",
            "CUSTOMER_3",
            "CUSTOMER_4",
            "CUSTOMER_5",
        ],
        "c_current_cdemo_sk": [1, 2, 3, 4, 5],
        "c_current_hdemo_sk": [1, 2, 3, 4, 5],
        "c_current_addr_sk": [1, 2, 3, 4, 5],
        "c_first_shipto_date_sk": [20240101, 20240102, 20240103, 20240104, 20240105],
        "c_first_sales_date_sk": [20240101, 20240102, 20240103, 20240104, 20240105],
        "c_salutation": ["Mr.", "Mrs.", "Ms.", "Dr.", "Mr."],
        "c_first_name": ["John", "Jane", "Bob", "Alice", "Charlie"],
        "c_last_name": ["Smith", "Johnson", "Williams", "Brown", "Davis"],
        "c_preferred_cust_flag": ["Y", "N", "Y", "Y", "N"],
        "c_birth_day": [15, 22, 8, 30, 12],
        "c_birth_month": [6, 3, 11, 7, 9],
        "c_birth_year": [1975, 1982, 1968, 1990, 1985],
        "c_birth_country": ["UNITED STATES", "UNITED STATES", "CANADA", "UNITED STATES", "MEXICO"],
        "c_login": ["jsmith", "jjohnson", "bwilliams", "abrown", "cdavis"],
        "c_email_address": ["john@email.com", "jane@email.com", "bob@email.com", "alice@email.com", "charlie@email.com"],
        "c_last_review_date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"],
    }
    return NlkDataFrame(data)

web_site = DeltalakeTable(
    name="web_site",
    uri="s3://my-bucket/tpc-ds/web_site",
    schema=pa.schema(
        [
            ("web_site_sk", pa.int64()),
            ("web_site_id", pa.string()),
            ("web_rec_start_date", pa.date32()),
            ("web_rec_end_date", pa.date32()),
            ("web_name", pa.string()),
            ("web_open_date_sk", pa.int64()),
            ("web_close_date_sk", pa.int64()),
            ("web_class", pa.string()),
            ("web_manager", pa.string()),
            ("web_mkt_id", pa.int64()),
            ("web_mkt_class", pa.string()),
            ("web_mkt_desc", pa.string()),
            ("web_market_manager", pa.string()),
            ("web_company_id", pa.int64()),
            ("web_company_name", pa.string()),
            ("web_street_number", pa.string()),
            ("web_street_name", pa.string()),
            ("web_street_type", pa.string()),
            ("web_suite_number", pa.string()),
            ("web_city", pa.string()),
            ("web_county", pa.string()),
            ("web_state", pa.string()),
            ("web_zip", pa.string()),
            ("web_country", pa.string()),
            ("web_gmt_offset", pa.float32()),
            ("web_tax_percentage", pa.float32()),
        ]
    ),
    docs_filters=[
        Filter("web_site_sk", "=", 1),
        Filter("web_class", "=", "E-Commerce"),
    ],
    unique_columns=["web_site_sk"],
    description="""
    Web site information from the TPC-DS benchmark.
    Contains web site details including management, marketing, and operational information.
    """,
    table_metadata_args={
        "data_input": "Web site configuration from digital commerce system",
        "latency_info": "Weekly updates from web operations team",
        "example_notebook": "https://example.com/notebooks/web_site_analysis.ipynb",
    },
)

store_returns = ParquetTable(
    name="store_returns",
    uri="s3://my-bucket/tpc-ds/store_returns",
    partitioning=[
        Partition(column="sr_item_sk", col_type=pl.Int64),
        Partition(column="sr_ticket_number", col_type=pl.Int64),
    ],
    partitioning_scheme=PartitioningScheme.HIVE,
    docs_filters=[
        Filter("sr_item_sk", "=", 1),
        Filter("sr_returned_date_sk", "=", 20240101),
    ],
    description="""
    Store returns information from the TPC-DS benchmark.
    Contains details about returned items including quantities, amounts, and fees.
    """,
    table_metadata_args={
        "data_input": "Store return transactions from POS and return processing systems",
        "latency_info": "Near real-time updates with 15-minute delay",
        "example_notebook": "https://example.com/notebooks/store_returns_analysis.ipynb",
    },
)

@table(
    data_input="Household demographic data from market research system <code>/api/demographics/households</code> endpoint",
    latency_info="Updated quarterly by the demographics_sync DAG on Airflow",
)
def household_demographics() -> NlkDataFrame:
    """Household demographics information from the TPC-DS benchmark."""
    data = {
        "hd_demo_sk": [1, 2, 3, 4, 5],
        "hd_income_band_sk": [1, 2, 3, 4, 5],
        "hd_buy_potential": ["HIGH", "MEDIUM", "LOW", "UNKNOWN", "HIGH"],
        "hd_dep_count": [2, 0, 3, 1, 4],
        "hd_vehicle_count": [1, 2, 2, 0, 3],
    }
    return NlkDataFrame(data)

web_page = DeltalakeTable(
    name="web_page",
    uri="s3://my-bucket/tpc-ds/web_page",
    schema=pa.schema(
        [
            ("wp_web_page_sk", pa.int64()),
            ("wp_web_page_id", pa.string()),
            ("wp_rec_start_date", pa.date32()),
            ("wp_rec_end_date", pa.date32()),
            ("wp_creation_date_sk", pa.int64()),
            ("wp_access_date_sk", pa.int64()),
            ("wp_autogen_flag", pa.string()),
            ("wp_customer_sk", pa.int64()),
            ("wp_url", pa.string()),
            ("wp_type", pa.string()),
            ("wp_char_count", pa.int64()),
            ("wp_link_count", pa.int64()),
            ("wp_image_count", pa.int64()),
            ("wp_max_ad_count", pa.int64()),
        ]
    ),
    docs_filters=[
        Filter("wp_web_page_sk", "=", 1),
        Filter("wp_type", "=", "product"),
    ],
    unique_columns=["wp_web_page_sk"],
    description="""
    Web page information from the TPC-DS benchmark.
    Contains web page details including content characteristics and access patterns.
    """,
    table_metadata_args={
        "data_input": "Web page metadata from content management system",
        "latency_info": "Daily updates from web analytics system",
        "example_notebook": "https://example.com/notebooks/web_page_analysis.ipynb",
    },
)

promotion = DeltalakeTable(
    name="promotion",
    uri="s3://my-bucket/tpc-ds/promotion",
    schema=pa.schema(
        [
            ("p_promo_sk", pa.int64()),
            ("p_promo_id", pa.string()),
            ("p_start_date_sk", pa.int64()),
            ("p_end_date_sk", pa.int64()),
            ("p_item_sk", pa.int64()),
            ("p_cost", pa.float64()),
            ("p_response_target", pa.int64()),
            ("p_promo_name", pa.string()),
            ("p_channel_dmail", pa.string()),
            ("p_channel_email", pa.string()),
            ("p_channel_catalog", pa.string()),
            ("p_channel_tv", pa.string()),
            ("p_channel_radio", pa.string()),
            ("p_channel_press", pa.string()),
            ("p_channel_event", pa.string()),
            ("p_channel_demo", pa.string()),
            ("p_channel_details", pa.string()),
            ("p_purpose", pa.string()),
            ("p_discount_active", pa.string()),
        ]
    ),
    docs_filters=[
        Filter("p_promo_sk", "=", 1),
        Filter("p_channel_email", "=", "Y"),
    ],
    unique_columns=["p_promo_sk"],
    description="""
    Promotion information from the TPC-DS benchmark.
    Contains promotional campaign details including channels, costs, and targeting information.
    """,
    table_metadata_args={
        "data_input": "Promotional campaign data from marketing automation system",
        "latency_info": "Daily updates from marketing campaign management system",
        "example_notebook": "https://example.com/notebooks/promotion_analysis.ipynb",
    },
)

catalog_page = DeltalakeTable(
    name="catalog_page",
    uri="s3://my-bucket/tpc-ds/catalog_page",
    schema=pa.schema(
        [
            ("cp_catalog_page_sk", pa.int64()),
            ("cp_catalog_page_id", pa.string()),
            ("cp_start_date_sk", pa.int64()),
            ("cp_end_date_sk", pa.int64()),
            ("cp_department", pa.string()),
            ("cp_catalog_number", pa.int64()),
            ("cp_catalog_page_number", pa.int64()),
            ("cp_description", pa.string()),
            ("cp_type", pa.string()),
        ]
    ),
    docs_filters=[
        Filter("cp_catalog_page_sk", "=", 1),
        Filter("cp_department", "=", "Electronics"),
    ],
    unique_columns=["cp_catalog_page_sk"],
    description="""
    Catalog page information from the TPC-DS benchmark.
    Contains catalog page details including department, page numbers, and descriptions.
    """,
    table_metadata_args={
        "data_input": "Catalog page metadata from catalog management system",
        "latency_info": "Weekly updates from catalog production system",
        "example_notebook": "https://example.com/notebooks/catalog_page_analysis.ipynb",
    },
)

inventory = ParquetTable(
    name="inventory",
    uri="s3://my-bucket/tpc-ds/inventory",
    partitioning=[
        Partition(column="inv_date_sk", col_type=pl.Int64),
        Partition(column="inv_item_sk", col_type=pl.Int64),
        Partition(column="inv_warehouse_sk", col_type=pl.Int64),
    ],
    partitioning_scheme=PartitioningScheme.HIVE,
    docs_filters=[
        Filter("inv_date_sk", "=", 20240101),
        Filter("inv_item_sk", "=", 1),
    ],
    description="""
    Inventory information from the TPC-DS benchmark.
    Contains inventory levels by date, item, and warehouse.
    """,
    table_metadata_args={
        "data_input": "Inventory levels from warehouse management system",
        "latency_info": "Daily inventory snapshots from WMS systems",
        "example_notebook": "https://example.com/notebooks/inventory_analysis.ipynb",
    },
)

catalog_returns = ParquetTable(
    name="catalog_returns",
    uri="s3://my-bucket/tpc-ds/catalog_returns",
    partitioning=[
        Partition(column="cr_item_sk", col_type=pl.Int64),
        Partition(column="cr_order_number", col_type=pl.Int64),
    ],
    partitioning_scheme=PartitioningScheme.HIVE,
    docs_filters=[
        Filter("cr_item_sk", "=", 1),
        Filter("cr_returned_date_sk", "=", 20240101),
    ],
    description="""
    Catalog returns information from the TPC-DS benchmark.
    Contains details about catalog order returns including quantities, amounts, and fees.
    """,
    table_metadata_args={
        "data_input": "Catalog return transactions from order management system",
        "latency_info": "Near real-time updates with 30-minute delay",
        "example_notebook": "https://example.com/notebooks/catalog_returns_analysis.ipynb",
    },
)

web_returns = ParquetTable(
    name="web_returns",
    uri="s3://my-bucket/tpc-ds/web_returns",
    partitioning=[
        Partition(column="wr_item_sk", col_type=pl.Int64),
        Partition(column="wr_order_number", col_type=pl.Int64),
    ],
    partitioning_scheme=PartitioningScheme.HIVE,
    docs_filters=[
        Filter("wr_item_sk", "=", 1),
        Filter("wr_returned_date_sk", "=", 20240101),
    ],
    description="""
    Web returns information from the TPC-DS benchmark.
    Contains details about web order returns including quantities, amounts, and fees.
    """,
    table_metadata_args={
        "data_input": "Web return transactions from e-commerce platform",
        "latency_info": "Near real-time updates with 15-minute delay",
        "example_notebook": "https://example.com/notebooks/web_returns_analysis.ipynb",
    },
)

web_sales = ParquetTable(
    name="web_sales",
    uri="s3://my-bucket/tpc-ds/web_sales",
    partitioning=[
        Partition(column="ws_item_sk", col_type=pl.Int64),
        Partition(column="ws_order_number", col_type=pl.Int64),
    ],
    partitioning_scheme=PartitioningScheme.HIVE,
    docs_filters=[
        Filter("ws_item_sk", "=", 1),
        Filter("ws_sold_date_sk", "=", 20240101),
    ],
    description="""
    Web sales information from the TPC-DS benchmark.
    Contains web sales transactions including pricing, discounts, taxes, and profit information.
    """,
    table_metadata_args={
        "data_input": "Web sales transactions from e-commerce platform",
        "latency_info": "Near real-time updates with 5-minute delay",
        "example_notebook": "https://example.com/notebooks/web_sales_analysis.ipynb",
    },
)

catalog_sales = ParquetTable(
    name="catalog_sales",
    uri="s3://my-bucket/tpc-ds/catalog_sales",
    partitioning=[
        Partition(column="cs_item_sk", col_type=pl.Int64),
        Partition(column="cs_order_number", col_type=pl.Int64),
    ],
    partitioning_scheme=PartitioningScheme.HIVE,
    docs_filters=[
        Filter("cs_item_sk", "=", 1),
        Filter("cs_sold_date_sk", "=", 20240101),
    ],
    description="""
    Catalog sales information from the TPC-DS benchmark.
    Contains catalog sales transactions including pricing, discounts, taxes, and profit information.
    """,
    table_metadata_args={
        "data_input": "Catalog sales transactions from order management system",
        "latency_info": "Near real-time updates with 10-minute delay",
        "example_notebook": "https://example.com/notebooks/catalog_sales_analysis.ipynb",
    },
)

store_sales = ParquetTable(
    name="store_sales",
    uri="s3://my-bucket/tpc-ds/store_sales",
    partitioning=[
        Partition(column="ss_item_sk", col_type=pl.Int64),
        Partition(column="ss_ticket_number", col_type=pl.Int64),
    ],
    partitioning_scheme=PartitioningScheme.HIVE,
    docs_filters=[
        Filter("ss_item_sk", "=", 1),
        Filter("ss_sold_date_sk", "=", 20240101),
    ],
    description="""
    Store sales information from the TPC-DS benchmark.
    Contains store sales transactions including pricing, discounts, taxes, and profit information.
    """,
    table_metadata_args={
        "data_input": "Store sales transactions from POS systems",
        "latency_info": "Near real-time updates with 5-minute delay",
        "example_notebook": "https://example.com/notebooks/store_sales_analysis.ipynb",
    },
) 