import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.config import conn_ids, email_lists, owners
from include.airflow.operators.snowflake_to_mssql import (
    SnowflakeSqlToMSSqlWatermarkOperator,
)
from include.utils.snowflake import Column


default_args = {
    "start_date": pendulum.datetime(2024, 3, 7, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_outbound_product_image_test",
    default_args=default_args,
    schedule="15,45 * * * *",
    catchup=False,
    max_active_tasks=1,
)

column_list = [
    Column(
        "PRODUCT_IMAGE_TEST_CONFIG_IMPORT_ID",
        "BIGINT",
        uniqueness=True,
        source_name="PRODUCT_IMAGE_TEST_CONFIG_EXPORT_ID",
    ),
    Column("MASTER_STORE_GROUP_ID", "INT"),
    Column("MASTER_PRODUCT_ID", "INT"),
    Column("PRODUCT_SKU", "VARCHAR(100)"),
    Column("MEMBERSHIP_BRAND_ID", "INT"),
    Column("EVALUATION_BATCH_ID", "INT"),
    Column("MASTER_TEST_NUMBER", "INT"),
    Column("CONFIG_JSON", "VARCHAR(MAX)"),
    Column("EVALUATION_MESSAGE", "VARCHAR(50)"),
    Column("EXTRA_PARAMS_JSON", "VARCHAR(MAX)"),
    Column("DATETIME_ADDED", "DATETIME"),
    Column("DATETIME_MODIFIED", "DATETIME"),
]

with dag:
    snowflake_to_mssql_dev = SnowflakeSqlToMSSqlWatermarkOperator(
        task_id=f"snowflake_to_mssql_product_image_test_dev",
        mssql_conn_id=conn_ids.MsSql.db166_app_airflow_rw,
        snowflake_conn_id=conn_ids.Snowflake.default,
        column_list=column_list,
        watermark_column="datetime_modified",
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="product_image_test_config_export",
        tgt_database="ultraimport",
        tgt_schema="dbo",
        tgt_table="product_image_test_config_import",
        if_exists="append",
        custom_sql="SELECT * FROM reporting_base_prod.shared.product_image_test_config_export",
    )

    snowflake_to_mssql_prod = SnowflakeSqlToMSSqlWatermarkOperator(
        task_id=f"snowflake_to_mssql_product_image_test_prod",
        mssql_conn_id=conn_ids.MsSql.db50_app_airflow_rw,
        snowflake_conn_id=conn_ids.Snowflake.default,
        column_list=column_list,
        watermark_column="datetime_modified",
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="product_image_test_config_export",
        tgt_database="ultraimport",
        tgt_schema="dbo",
        tgt_table="product_image_test_config_import",
        if_exists="append",
        custom_sql="SELECT * FROM reporting_base_prod.shared.product_image_test_config_export",
    )
