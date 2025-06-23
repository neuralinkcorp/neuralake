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
from examples.tpc_catalog import TPCCatalog
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
    # TPC-DS tables
    elif self.name == "customer_address":
        schema = pa.schema([
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
        ])
        partitions = [{"column_name": "ca_address_sk", "type_annotation": "int", "value": 1}]
    elif self.name == "customer_demographics":
        schema = pa.schema([
            ("cd_demo_sk", pa.int64()),
            ("cd_gender", pa.string()),
            ("cd_marital_status", pa.string()),
            ("cd_education_status", pa.string()),
            ("cd_purchase_estimate", pa.int64()),
            ("cd_credit_rating", pa.string()),
            ("cd_dep_count", pa.int64()),
            ("cd_dep_employed_count", pa.int64()),
            ("cd_dep_college_count", pa.int64()),
        ])
        partitions = [{"column_name": "cd_demo_sk", "type_annotation": "int", "value": 1}]
    elif self.name == "date_dim":
        schema = pa.schema([
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
        ])
        partitions = [{"column_name": "d_date_sk", "type_annotation": "int", "value": 1}]
    elif self.name == "warehouse":
        schema = pa.schema([
            ("w_warehouse_sk", pa.int64()),
            ("w_warehouse_id", pa.string()),
            ("w_warehouse_name", pa.string()),
            ("w_warehouse_sq_ft", pa.int64()),
            ("w_street_number", pa.string()),
            ("w_street_name", pa.string()),
            ("w_street_type", pa.string()),
            ("w_suite_number", pa.string()),
            ("w_city", pa.string()),
            ("w_county", pa.string()),
            ("w_state", pa.string()),
            ("w_zip", pa.string()),
            ("w_country", pa.string()),
            ("w_gmt_offset", pa.float32()),
        ])
        partitions = [{"column_name": "w_warehouse_sk", "type_annotation": "int", "value": 1}]
    elif self.name == "ship_mode":
        schema = pa.schema([
            ("sm_ship_mode_sk", pa.int64()),
            ("sm_ship_mode_id", pa.string()),
            ("sm_type", pa.string()),
            ("sm_code", pa.string()),
            ("sm_carrier", pa.string()),
            ("sm_contract", pa.string()),
        ])
        partitions = [{"column_name": "sm_ship_mode_sk", "type_annotation": "int", "value": 1}]
    elif self.name == "time_dim":
        schema = pa.schema([
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
        ])
        partitions = [{"column_name": "t_time_sk", "type_annotation": "int", "value": 1}]
    elif self.name == "reason":
        schema = pa.schema([
            ("r_reason_sk", pa.int64()),
            ("r_reason_id", pa.string()),
            ("r_reason_desc", pa.string()),
        ])
        partitions = [{"column_name": "r_reason_sk", "type_annotation": "int", "value": 1}]
    elif self.name == "income_band":
        schema = pa.schema([
            ("ib_income_band_sk", pa.int64()),
            ("ib_lower_bound", pa.int64()),
            ("ib_upper_bound", pa.int64()),
        ])
        partitions = [{"column_name": "ib_income_band_sk", "type_annotation": "int", "value": 1}]
    elif self.name == "item":
        schema = pa.schema([
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
        ])
        partitions = [{"column_name": "i_item_sk", "type_annotation": "int", "value": 1}]
    elif self.name == "store":
        schema = pa.schema([
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
        ])
        partitions = [{"column_name": "s_store_sk", "type_annotation": "int", "value": 1}]
    elif self.name == "call_center":
        schema = pa.schema([
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
        ])
        partitions = [{"column_name": "cc_call_center_sk", "type_annotation": "int", "value": 1}]
    elif self.name == "customer":
        # Handle both TPC-H and TPC-DS customer tables
        if hasattr(self, 'schema') and any('c_customer_sk' in str(field) for field in self.schema):
            # TPC-DS customer
            schema = pa.schema([
                ("c_customer_sk", pa.int64()),
                ("c_customer_id", pa.string()),
                ("c_current_cdemo_sk", pa.int64()),
                ("c_current_hdemo_sk", pa.int64()),
                ("c_current_addr_sk", pa.int64()),
                ("c_first_shipto_date_sk", pa.int64()),
                ("c_first_sales_date_sk", pa.int64()),
                ("c_salutation", pa.string()),
                ("c_first_name", pa.string()),
                ("c_last_name", pa.string()),
                ("c_preferred_cust_flag", pa.string()),
                ("c_birth_day", pa.int64()),
                ("c_birth_month", pa.int64()),
                ("c_birth_year", pa.int64()),
                ("c_birth_country", pa.string()),
                ("c_login", pa.string()),
                ("c_email_address", pa.string()),
                ("c_last_review_date", pa.string()),
            ])
            partitions = [{"column_name": "c_customer_sk", "type_annotation": "int", "value": 1}]
        else:
            # TPC-H customer (original)
            schema = pa.schema([
                ("c_custkey", pa.int64()),
                ("c_name", pa.string()),
                ("c_address", pa.string()),
                ("c_nationkey", pa.int64()),
                ("c_phone", pa.string()),
                ("c_acctbal", pa.decimal128(12, 2)),
                ("c_mktsegment", pa.string()),
                ("c_comment", pa.string()),
            ])
            partitions = [{"column_name": "c_custkey", "type_annotation": "int", "value": 1}]
    elif self.name == "web_site":
        schema = pa.schema([
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
        ])
        partitions = [{"column_name": "web_site_sk", "type_annotation": "int", "value": 1}]
    elif self.name == "store_returns":
        schema = pa.schema([
            ("sr_returned_date_sk", pa.int64()),
            ("sr_return_time_sk", pa.int64()),
            ("sr_item_sk", pa.int64()),
            ("sr_customer_sk", pa.int64()),
            ("sr_cdemo_sk", pa.int64()),
            ("sr_hdemo_sk", pa.int64()),
            ("sr_addr_sk", pa.int64()),
            ("sr_store_sk", pa.int64()),
            ("sr_reason_sk", pa.int64()),
            ("sr_ticket_number", pa.int64()),
            ("sr_return_quantity", pa.int64()),
            ("sr_return_amt", pa.float32()),
            ("sr_return_tax", pa.float32()),
            ("sr_return_amt_inc_tax", pa.float32()),
            ("sr_fee", pa.float32()),
            ("sr_return_ship_cost", pa.float32()),
            ("sr_refunded_cash", pa.float32()),
            ("sr_reversed_charge", pa.float32()),
            ("sr_store_credit", pa.float32()),
            ("sr_net_loss", pa.float32()),
        ])
        partitions = [
            {"column_name": "sr_item_sk", "type_annotation": "int", "value": 1},
            {"column_name": "sr_ticket_number", "type_annotation": "int", "value": 1},
        ]
    elif self.name == "household_demographics":
        schema = pa.schema([
            ("hd_demo_sk", pa.int64()),
            ("hd_income_band_sk", pa.int64()),
            ("hd_buy_potential", pa.string()),
            ("hd_dep_count", pa.int64()),
            ("hd_vehicle_count", pa.int64()),
        ])
        partitions = [{"column_name": "hd_demo_sk", "type_annotation": "int", "value": 1}]
    elif self.name == "web_page":
        schema = pa.schema([
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
        ])
        partitions = [{"column_name": "wp_web_page_sk", "type_annotation": "int", "value": 1}]
    elif self.name == "promotion":
        schema = pa.schema([
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
        ])
        partitions = [{"column_name": "p_promo_sk", "type_annotation": "int", "value": 1}]
    elif self.name == "catalog_page":
        schema = pa.schema([
            ("cp_catalog_page_sk", pa.int64()),
            ("cp_catalog_page_id", pa.string()),
            ("cp_start_date_sk", pa.int64()),
            ("cp_end_date_sk", pa.int64()),
            ("cp_department", pa.string()),
            ("cp_catalog_number", pa.int64()),
            ("cp_catalog_page_number", pa.int64()),
            ("cp_description", pa.string()),
            ("cp_type", pa.string()),
        ])
        partitions = [{"column_name": "cp_catalog_page_sk", "type_annotation": "int", "value": 1}]
    elif self.name == "inventory":
        schema = pa.schema([
            ("inv_date_sk", pa.int64()),
            ("inv_item_sk", pa.int64()),
            ("inv_warehouse_sk", pa.int64()),
            ("inv_quantity_on_hand", pa.int64()),
        ])
        partitions = [
            {"column_name": "inv_date_sk", "type_annotation": "int", "value": 1},
            {"column_name": "inv_item_sk", "type_annotation": "int", "value": 1},
            {"column_name": "inv_warehouse_sk", "type_annotation": "int", "value": 1},
        ]
    elif self.name == "catalog_returns":
        schema = pa.schema([
            ("cr_returned_date_sk", pa.int64()),
            ("cr_returned_time_sk", pa.int64()),
            ("cr_item_sk", pa.int64()),
            ("cr_refunded_customer_sk", pa.int64()),
            ("cr_refunded_cdemo_sk", pa.int64()),
            ("cr_refunded_hdemo_sk", pa.int64()),
            ("cr_refunded_addr_sk", pa.int64()),
            ("cr_returning_customer_sk", pa.int64()),
            ("cr_returning_cdemo_sk", pa.int64()),
            ("cr_returning_hdemo_sk", pa.int64()),
            ("cr_returning_addr_sk", pa.int64()),
            ("cr_call_center_sk", pa.int64()),
            ("cr_catalog_page_sk", pa.int64()),
            ("cr_ship_mode_sk", pa.int64()),
            ("cr_warehouse_sk", pa.int64()),
            ("cr_reason_sk", pa.int64()),
            ("cr_order_number", pa.int64()),
            ("cr_return_quantity", pa.int64()),
            ("cr_return_amount", pa.float32()),
            ("cr_return_tax", pa.float32()),
            ("cr_return_amt_inc_tax", pa.float32()),
            ("cr_fee", pa.float32()),
            ("cr_return_ship_cost", pa.float32()),
            ("cr_refunded_cash", pa.float32()),
            ("cr_reversed_charge", pa.float32()),
            ("cr_store_credit", pa.float32()),
            ("cr_net_loss", pa.float32()),
        ])
        partitions = [
            {"column_name": "cr_item_sk", "type_annotation": "int", "value": 1},
            {"column_name": "cr_order_number", "type_annotation": "int", "value": 1},
        ]
    elif self.name == "web_returns":
        schema = pa.schema([
            ("wr_returned_date_sk", pa.int64()),
            ("wr_returned_time_sk", pa.int64()),
            ("wr_item_sk", pa.int64()),
            ("wr_refunded_customer_sk", pa.int64()),
            ("wr_refunded_cdemo_sk", pa.int64()),
            ("wr_refunded_hdemo_sk", pa.int64()),
            ("wr_refunded_addr_sk", pa.int64()),
            ("wr_returning_customer_sk", pa.int64()),
            ("wr_returning_cdemo_sk", pa.int64()),
            ("wr_returning_hdemo_sk", pa.int64()),
            ("wr_returning_addr_sk", pa.int64()),
            ("wr_web_page_sk", pa.int64()),
            ("wr_reason_sk", pa.int64()),
            ("wr_order_number", pa.int64()),
            ("wr_return_quantity", pa.int64()),
            ("wr_return_amt", pa.float32()),
            ("wr_return_tax", pa.float32()),
            ("wr_return_amt_inc_tax", pa.float32()),
            ("wr_fee", pa.float32()),
            ("wr_return_ship_cost", pa.float32()),
            ("wr_refunded_cash", pa.float32()),
            ("wr_reversed_charge", pa.float32()),
            ("wr_account_credit", pa.float32()),
            ("wr_net_loss", pa.float32()),
        ])
        partitions = [
            {"column_name": "wr_item_sk", "type_annotation": "int", "value": 1},
            {"column_name": "wr_order_number", "type_annotation": "int", "value": 1},
        ]
    elif self.name == "web_sales":
        schema = pa.schema([
            ("ws_sold_date_sk", pa.int64()),
            ("ws_sold_time_sk", pa.int64()),
            ("ws_ship_date_sk", pa.int64()),
            ("ws_item_sk", pa.int64()),
            ("ws_bill_customer_sk", pa.int64()),
            ("ws_bill_cdemo_sk", pa.int64()),
            ("ws_bill_hdemo_sk", pa.int64()),
            ("ws_bill_addr_sk", pa.int64()),
            ("ws_ship_customer_sk", pa.int64()),
            ("ws_ship_cdemo_sk", pa.int64()),
            ("ws_ship_hdemo_sk", pa.int64()),
            ("ws_ship_addr_sk", pa.int64()),
            ("ws_web_page_sk", pa.int64()),
            ("ws_web_site_sk", pa.int64()),
            ("ws_ship_mode_sk", pa.int64()),
            ("ws_warehouse_sk", pa.int64()),
            ("ws_promo_sk", pa.int64()),
            ("ws_order_number", pa.int64()),
            ("ws_quantity", pa.int64()),
            ("ws_wholesale_cost", pa.float32()),
            ("ws_list_price", pa.float32()),
            ("ws_sales_price", pa.float32()),
            ("ws_ext_discount_amt", pa.float32()),
            ("ws_ext_sales_price", pa.float32()),
            ("ws_ext_wholesale_cost", pa.float32()),
            ("ws_ext_list_price", pa.float32()),
            ("ws_ext_tax", pa.float32()),
            ("ws_coupon_amt", pa.float32()),
            ("ws_ext_ship_cost", pa.float32()),
            ("ws_net_paid", pa.float32()),
            ("ws_net_paid_inc_tax", pa.float32()),
            ("ws_net_paid_inc_ship", pa.float32()),
            ("ws_net_paid_inc_ship_tax", pa.float32()),
            ("ws_net_profit", pa.float32()),
        ])
        partitions = [
            {"column_name": "ws_item_sk", "type_annotation": "int", "value": 1},
            {"column_name": "ws_order_number", "type_annotation": "int", "value": 1},
        ]
    elif self.name == "catalog_sales":
        schema = pa.schema([
            ("cs_sold_date_sk", pa.int64()),
            ("cs_sold_time_sk", pa.int64()),
            ("cs_ship_date_sk", pa.int64()),
            ("cs_bill_customer_sk", pa.int64()),
            ("cs_bill_cdemo_sk", pa.int64()),
            ("cs_bill_hdemo_sk", pa.int64()),
            ("cs_bill_addr_sk", pa.int64()),
            ("cs_ship_customer_sk", pa.int64()),
            ("cs_ship_cdemo_sk", pa.int64()),
            ("cs_ship_hdemo_sk", pa.int64()),
            ("cs_ship_addr_sk", pa.int64()),
            ("cs_call_center_sk", pa.int64()),
            ("cs_catalog_page_sk", pa.int64()),
            ("cs_ship_mode_sk", pa.int64()),
            ("cs_warehouse_sk", pa.int64()),
            ("cs_item_sk", pa.int64()),
            ("cs_promo_sk", pa.int64()),
            ("cs_order_number", pa.int64()),
            ("cs_quantity", pa.int64()),
            ("cs_wholesale_cost", pa.float32()),
            ("cs_list_price", pa.float32()),
            ("cs_sales_price", pa.float32()),
            ("cs_ext_discount_amt", pa.float32()),
            ("cs_ext_sales_price", pa.float32()),
            ("cs_ext_wholesale_cost", pa.float32()),
            ("cs_ext_list_price", pa.float32()),
            ("cs_ext_tax", pa.float32()),
            ("cs_coupon_amt", pa.float32()),
            ("cs_ext_ship_cost", pa.float32()),
            ("cs_net_paid", pa.float32()),
            ("cs_net_paid_inc_tax", pa.float32()),
            ("cs_net_paid_inc_ship", pa.float32()),
            ("cs_net_paid_inc_ship_tax", pa.float32()),
            ("cs_net_profit", pa.float32()),
        ])
        partitions = [
            {"column_name": "cs_item_sk", "type_annotation": "int", "value": 1},
            {"column_name": "cs_order_number", "type_annotation": "int", "value": 1},
        ]
    elif self.name == "store_sales":
        schema = pa.schema([
            ("ss_sold_date_sk", pa.int64()),
            ("ss_sold_time_sk", pa.int64()),
            ("ss_item_sk", pa.int64()),
            ("ss_customer_sk", pa.int64()),
            ("ss_cdemo_sk", pa.int64()),
            ("ss_hdemo_sk", pa.int64()),
            ("ss_addr_sk", pa.int64()),
            ("ss_store_sk", pa.int64()),
            ("ss_promo_sk", pa.int64()),
            ("ss_ticket_number", pa.int64()),
            ("ss_quantity", pa.int64()),
            ("ss_wholesale_cost", pa.float32()),
            ("ss_list_price", pa.float32()),
            ("ss_sales_price", pa.float32()),
            ("ss_ext_discount_amt", pa.float32()),
            ("ss_ext_sales_price", pa.float32()),
            ("ss_ext_wholesale_cost", pa.float32()),
            ("ss_ext_list_price", pa.float32()),
            ("ss_ext_tax", pa.float32()),
            ("ss_coupon_amt", pa.float32()),
            ("ss_net_paid", pa.float32()),
            ("ss_net_paid_inc_tax", pa.float32()),
            ("ss_net_profit", pa.float32()),
        ])
        partitions = [
            {"column_name": "ss_item_sk", "type_annotation": "int", "value": 1},
            {"column_name": "ss_ticket_number", "type_annotation": "int", "value": 1},
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
    parser = argparse.ArgumentParser(description="Generate TPC web catalog with TPC-H and TPC-DS databases")
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
        # Export and generate the site with the unified TPC catalog
        export_and_generate_site(
            catalogs=[("tpc", TPCCatalog)], output_dir=str(output_dir)
        )

    print(f"Static site generated at: {output_dir}")


if __name__ == "__main__":
    main()
