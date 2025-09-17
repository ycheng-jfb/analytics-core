import pendulum

from dataclasses import dataclass
from datetime import timedelta
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.amazon import AmazonToS3Operator
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.snowflake_load import (
    Column,
    CopyConfigCsv,
    SnowflakeIncrementalLoadOperator,
)
from include.config import email_lists, owners, s3_buckets, stages, conn_ids


@dataclass
class Config:
    report_type: str
    is_request: bool
    column_list: list
    market_place_ids: list
    table_name: str
    file_extension: str
    priority_weight: int


report_config = [
    Config(
        report_type='GET_FBA_INVENTORY_PLANNING_DATA',
        is_request=True,
        market_place_ids=["A2EUQ1WTGCTBG2", "ATVPDKIKX0DER", "A1AM78C64UM0Y8", "A2Q3Y263D00KWC"],
        table_name='selling_partner_fba_inventory_planning_data',
        file_extension='gz',
        priority_weight=50,
        column_list=[
            Column("snapshot_date", "DATE"),
            Column("sku", "VARCHAR", uniqueness=True),
            Column("fnsku", "VARCHAR"),
            Column("asin", "VARCHAR"),
            Column("product_name", "VARCHAR"),
            Column("condition", "VARCHAR"),
            Column("available", "INT"),
            Column("pending_removal_quantity", "INT"),
            Column("inv_age_0_to_90_days", "INT"),
            Column("inv_age_91_to_180_days", "INT"),
            Column("inv_age_181_to_270_days", "INT"),
            Column("inv_age_271_to_365_days", "INT"),
            Column("inv_age_365_plus_days", "INT"),
            Column("currency", "VARCHAR"),
            Column("units_shipped_t7", "INT"),
            Column("units_shipped_t30", "INT"),
            Column("units_shipped_t60", "INT"),
            Column("units_shipped_t90", "INT"),
            Column("alert", "VARCHAR"),
            Column("your_price", "NUMBER(19,4)"),
            Column("sales_price", "NUMBER(19,4)"),
            Column("lowest_price_new_plus_shipping", "NUMBER(19,4)"),
            Column("lowest_price_used", "NUMBER(19,4)"),
            Column("recommended_action", "VARCHAR"),
            Column("deprecated_healthy_inventory_level", "VARCHAR"),
            Column("recommended_sales_price", "NUMBER(19,4)"),
            Column("recommended_sale_duration_days", "INT"),
            Column("recommended_removal_quantity", "INT"),
            Column("estimated_cost_savings_of_recommended_actions", "NUMBER(19,4)"),
            Column("sell_through", "NUMBER(19,4)"),
            Column("item_volume", "NUMBER(19,6)"),
            Column("volume_unit_measurement", "VARCHAR"),
            Column("storage_type", "VARCHAR"),
            Column("storage_volume", "NUMBER(19,6)"),
            Column("marketplace", "VARCHAR"),
            Column("product_group", "VARCHAR"),
            Column("sales_rank", "INT"),
            Column("days_of_supply", "INT"),
            Column("estimated_excess_quantity", "INT"),
            Column("weeks_of_cover_t30", "INT"),
            Column("weeks_of_cover_t90", "INT"),
            Column("featuredoffer_price", "NUMBER(19,4)"),
            Column("sales_shipped_last_7_days", "NUMBER(19,4)"),
            Column("sales_shipped_last_30_days", "NUMBER(19,4)"),
            Column("sales_shipped_last_60_days", "NUMBER(19,4)"),
            Column("sales_shipped_last_90_days", "NUMBER(19,4)"),
            Column("inv_age_0_to_30_days", "INT"),
            Column("inv_age_31_to_60_days", "INT"),
            Column("inv_age_61_to_90_days", "INT"),
            Column("inv_age_181_to_330_days", "INT"),
            Column("inv_age_331_to_365_days", "INT"),
            Column("estimated_storage_cost_next_month", "NUMBER(19,4)"),
            Column("inbound_quantity", "INT"),
            Column("inbound_working", "INT"),
            Column("inbound_shipped", "INT"),
            Column("inbound_received", "INT"),
            Column("no_sale_last_6_months", "INT"),
            Column("reserved_quantity", "INT"),
            Column("unfulfillable_quantity", "INT"),
            Column("quantity_to_be_charged_ais_181_210_days", "INT"),
            Column("estimated_ais_181_210_days", "INT"),
            Column("quantity_to_be_charged_ais_211_240_days", "INT"),
            Column("estimated_ais_211_240_days", "NUMBER(19,4)"),
            Column("quantity_to_be_charged_ais_241_270_days", "INT"),
            Column("estimated_ais_241_270_days", "NUMBER(19,4)"),
            Column("quantity_to_be_charged_ais_271_300_days", "INT"),
            Column("estimated_ais_271_300_days", "NUMBER(19,4)"),
            Column("quantity_to_be_charged_ais_301_330_days", "INT"),
            Column("estimated_ais_301_330_days", "NUMBER(19,4)"),
            Column("quantity_to_be_charged_ais_331_365_days", "INT"),
            Column("estimated_ais_331_365_days", "NUMBER(19,4)"),
            Column("quantity_to_be_charged_ais_365_plus_days", "INT"),
            Column("estimated_ais_365_plus_days", "NUMBER(19,4)"),
            Column("historical_days_of_supply", "NUMBER(19,4)"),
            Column("fba_minimum_inventory_level", "INT"),
            Column("fba_inventory_level_health_status", "VARCHAR"),
            Column("recommended_ship_in_quantity", "INT"),
            Column("recommended_ship_in_date", "DATE"),
        ],
    ),
    Config(
        report_type="GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2",
        is_request=False,
        market_place_ids=[],
        table_name='selling_partner_settlement_report_data',
        file_extension='tsv',
        priority_weight=10,
        column_list=[
            Column("settlement_id", "INT"),
            Column("settlement_start_date", "TIMESTAMP_NTZ"),
            Column("settlement_end_date", "TIMESTAMP_NTZ"),
            Column("deposit_date", "TIMESTAMP_NTZ"),
            Column("total_amount", "NUMBER(19,4)"),
            Column("currency", "VARCHAR"),
            Column("transaction_type", "VARCHAR"),
            Column("order_id", "VARCHAR"),
            Column("merchant_order_id", "VARCHAR"),
            Column("adjustment_id", "VARCHAR"),
            Column("shipment_id", "VARCHAR"),
            Column("marketplace_name", "VARCHAR"),
            Column("amount_type", "VARCHAR"),
            Column("amount_description", "VARCHAR"),
            Column("amount", "NUMBER(19,4)"),
            Column("fulfillment_id", "VARCHAR"),
            Column("posted_date", "DATE"),
            Column("posted_date_time", "TIMESTAMP_NTZ"),
            Column("order_item_code", "VARCHAR"),
            Column("merchant_order_item_id", "VARCHAR"),
            Column("merchant_adjustment_item_id", "VARCHAR"),
            Column("sku", "VARCHAR"),
            Column("quantity_purchased", "INT"),
            Column("promotion_id", "VARCHAR"),
        ],
    ),
    Config(
        report_type="GET_AFN_INVENTORY_DATA",
        is_request=True,
        market_place_ids=["A2EUQ1WTGCTBG2", "ATVPDKIKX0DER", "A1AM78C64UM0Y8", "A2Q3Y263D00KWC"],
        table_name='selling_partner_inventory_data',
        file_extension='tsv',
        priority_weight=100,
        column_list=[
            Column("SELLER_SKU", "VARCHAR", uniqueness=True),
            Column("FULFILLMENT_CHANNEL_SKU", "VARCHAR"),
            Column("ASIN", "VARCHAR", uniqueness=True),
            Column("CONDITION_TYPE", "VARCHAR"),
            Column("WAREHOUSE_CONDITION_CODE", "VARCHAR", uniqueness=True),
            Column("QUANTITY_AVAILABLE", "INT"),
        ],
    ),
    Config(
        report_type="GET_MERCHANT_LISTINGS_ALL_DATA",
        is_request=True,
        market_place_ids=["A2EUQ1WTGCTBG2", "ATVPDKIKX0DER", "A1AM78C64UM0Y8", "A2Q3Y263D00KWC"],
        table_name="selling_partner_merchant_listings",
        file_extension='tsv',
        priority_weight=10,
        column_list=[],
    ),
]


default_args = {
    "start_date": pendulum.datetime(2021, 8, 23, 7, tz="America/Los_Angeles"),
    "retries": 1,
    'owner': owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
    "execution_timeout": timedelta(hours=1),
}


dag = DAG(
    dag_id=f"edm_inbound_amazon_selling_partner_reports",
    default_args=default_args,
    schedule="30 3 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
)


with dag:
    schema = "amazon"
    start_time = "{{ (prev_data_interval_start_success or data_interval_start).strftime('%Y-%m-%dT%H:%M:%S%z') }}"
    settlement_report_data = SnowflakeProcedureOperator(
        procedure='amazon.settlement_report_data.sql',
        database='lake',
        autocommit=False,
    )
    for report in report_config:
        s3_prefix = f"lake/{schema}.{report.table_name}/daily_v1"

        if report.table_name == "selling_partner_merchant_listings":
            merchant_data_to_snowflake = SnowflakeProcedureOperator(
                procedure='amazon.selling_partner_merchant_listings.sql',
                database='lake',
                autocommit=False,
            )
            for market_place_id in report.market_place_ids:
                merchant_data_to_s3 = AmazonToS3Operator(
                    task_id=f"amazon_{report.table_name}_{market_place_id}_to_s3",
                    bucket=s3_buckets.tsos_da_int_inbound,
                    s3_prefix=s3_prefix,
                    s3_conn_id=conn_ids.S3.tsos_da_int_prod,
                    report_type=report.report_type,
                    market_place_ids=[market_place_id],
                    is_request=report.is_request,
                    start_time=start_time,
                    file_extension=report.file_extension,
                    priority_weight=report.priority_weight,
                )
                merchant_data_to_s3 >> merchant_data_to_snowflake
        else:
            get_data = AmazonToS3Operator(
                task_id=f"amazon_{report.table_name}_to_s3",
                bucket=s3_buckets.tsos_da_int_inbound,
                s3_prefix=s3_prefix,
                s3_conn_id=conn_ids.S3.tsos_da_int_prod,
                report_type=report.report_type,
                market_place_ids=report.market_place_ids,
                is_request=report.is_request,
                start_time=start_time,
                file_extension=report.file_extension,
                priority_weight=report.priority_weight,
            )
            if report.is_request:
                load_to_snowflake = SnowflakeIncrementalLoadOperator(
                    task_id=f"amazon_{report.table_name}_to_snowflake",
                    database="lake",
                    schema=schema,
                    table=report.table_name,
                    column_list=report.column_list,
                    files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}",
                    copy_config=CopyConfigCsv(field_delimiter='\t', header_rows=1, skip_pct=3),
                )
                get_data >> load_to_snowflake
            else:
                get_data >> settlement_report_data
