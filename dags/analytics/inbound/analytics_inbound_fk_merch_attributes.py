import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.excel_smb import ExcelSMBToS3Operator, SheetConfig
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.config import conn_ids, email_lists, owners, s3_buckets
from include.utils.snowflake import Column

sheets = [
    SheetConfig(
        sheet_name="Item Master",
        schema="excel",
        table="fk_merch_item_attributes",
        header_rows=5,
        column_list=[
            Column("style_color", "STRING"),
            Column("prod_id", "NUMBER(38,0)"),
            Column("category", "STRING"),
            Column("gender", "STRING"),
            Column("class", "STRING"),
            Column("subclass", "STRING"),
            Column("attribute", "STRING"),
            Column("taxonomy_global_category", "STRING"),
            Column("taxonomy_class", "STRING"),
            Column("site_name", "STRING"),
            Column("color", "STRING"),
            Column("size_model", "STRING"),
            Column("number_of_sizes", "NUMBER(38,0)"),
            Column("true_cost", "NUMBER(38,2)"),
            Column("tariff_cost", "NUMBER(38,2)"),
            Column("landed_cost", "NUMBER(38,2)"),
            Column("sale_price", "NUMBER(38,2)"),
            Column("vip_price", "NUMBER(38,2)"),
            Column("retail", "NUMBER(38,2)"),
            Column("imu_percent", "NUMBER(38,2)"),
            Column("new_season_code", "STRING"),
            Column("ro_rcvd_date", "STRING"),
            Column("age", "NUMBER(38,0)"),
            Column("retail_status", "STRING"),
            Column("initial_showroom", "STRING"),
            Column("year", "NUMBER(38,0)"),
            Column("month", "STRING"),
            Column("launch_date", "DATE"),
            Column("reintro_launch_date", "STRING"),
            Column("ttl_rcvd_qty", "NUMBER(38,0)"),
            Column("total_oh_qty", "NUMBER(38,0)"),
            Column("tariff_flag", "STRING"),
            Column("active_inactive", "STRING"),
            Column("notes", "STRING"),
        ],
    ),
    SheetConfig(
        sheet_name="Outfit Master",
        schema="excel",
        table="fk_merch_outfit_attributes",
        header_rows=4,
        column_list=[
            Column("product_id", "NUMBER(38,0)"),
            Column("outfit_alias", "STRING"),
            Column("site_name", "STRING"),
            Column("vip_retail", "NUMBER(19,2)"),
            Column("type", "STRING"),
            Column("gender", "STRING"),
            Column("active_inactive", "STRING"),
            Column("outfit_vs_box_vs_pack", "STRING"),
            Column("number_of_components", "NUMBER(38,0)"),
            Column("item1", "STRING"),
            Column("item2", "STRING"),
            Column("item3", "STRING"),
            Column("item4", "STRING"),
            Column("item5", "STRING"),
            Column("item6", "STRING"),
            Column("item7", "STRING"),
            Column("item8", "STRING"),
            Column("total_cost_of_outfit", "NUMBER(38,2)"),
            Column("total_cost_of_outfit_tariff", "NUMBER(38,2)"),
            Column("outfit_imu_percent", "NUMBER(38,2)"),
            Column("outfit_imu_percent_tariff", "NUMBER(38,2)"),
            Column("contains_shoes", "STRING"),
        ],
    ),
]

default_args = {
    "start_date": pendulum.datetime(2020, 3, 1, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="analytics_inbound_fk_merch_attributes",
    default_args=default_args,
    schedule="0 9 * * *",
    catchup=False,
    max_active_tasks=100,
    max_active_runs=1,
)

with dag:
    to_s3 = ExcelSMBToS3Operator(
        task_id="excel_to_s3",
        smb_path="Inbound/merch_fabkids_master/Item and Outfit Master.xlsx",
        share_name="BI",
        bucket=s3_buckets.tsos_da_int_inbound,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        smb_conn_id=conn_ids.SMB.nas01,
        sheet_configs=sheets,
        remove_header_new_lines=True,
        default_schema_version="v2",
    )

    fk_item_attributes_to_snowflake = SnowflakeProcedureOperator(
        database="lake",
        procedure="excel.fk_merch_item_attributes.sql",
        warehouse="DA_WH_ETL_LIGHT",
    )

    fk_outfit_attributes_to_snowflake = SnowflakeProcedureOperator(
        database="lake",
        procedure="excel.fk_merch_outfit_attributes.sql",
        warehouse="DA_WH_ETL_LIGHT",
    )

    to_s3 >> [fk_item_attributes_to_snowflake, fk_outfit_attributes_to_snowflake]
