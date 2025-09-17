import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake_load import SnowflakeIncrementalLoadOperator
from dags.include.config import owners, snowflake_roles, stages
from dags.include.config.email_lists import data_integration_support
from include.utils.snowflake import Column, CopyConfigCsv

default_args = {
    "start_date": pendulum.datetime(2020, 10, 4, tz="America/Los_Angeles"),
    "retries": 0,
    "owner": owners.data_integrations,
    "email": data_integration_support,
    "on_failure_callback": slack_failure_edm,
}


dag = DAG(
    dag_id="edm_inbound_oracle_ebs_landed_cost",
    default_args=default_args,
    schedule="30 0 * * *",
    catchup=False,
    max_active_tasks=1000,
    max_active_runs=1,
)

s3_prefix = "inbound/svc_oracle_ebs/lake.oracle_ebs.landed_cost"

column_list = [
    Column("ledger", "VARCHAR(100)"),
    Column("legal_entity", "VARCHAR(100)"),
    Column("operating_unit", "VARCHAR(100)"),
    Column("org_code", "VARCHAR(10)"),
    Column("org_name", "VARCHAR(200)"),
    Column("transaction_date", "DATE"),
    Column("costed_date", "DATE"),
    Column("transaction_type_name", "VARCHAR(100)"),
    Column("sales_order", "INT"),
    Column("line_number", "INT"),
    Column("store_id", "INT"),
    Column("item_number", "VARCHAR(100)"),
    Column("item_description", "VARCHAR(200)"),
    Column("primary_quantity", "INT"),
    Column("transaction_cost", "NUMBER(19, 4)"),
    Column("unit_cost", "NUMBER(18, 4)"),
    Column("currency_code", "VARCHAR(20)"),
    Column("conversion_rate", "NUMBER(18, 6)"),
    Column("gl_company", "VARCHAR(50)"),
    Column("gl_brand", "VARCHAR(50)"),
    Column("gl_region", "VARCHAR(50)"),
    Column("gl_channel", "VARCHAR(50)"),
    Column("gl_department", "VARCHAR(50)"),
    Column("gl_account", "VARCHAR(50)"),
    Column("gl_intercompany", "VARCHAR(50)"),
    Column("gl_future", "VARCHAR(50)"),
    Column("mmt_pk", "INT", uniqueness=True),
    Column("mta_pk", "INT"),
    Column("costed_date_filter", "TIMESTAMP_NTZ(3)"),
]

with dag:
    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="oracle_ebs_s3_to_snowflake",
        database="lake",
        schema="oracle_ebs",
        table="landed_cost",
        staging_database="lake_stg",
        snowflake_conn_id="snowflake_default",
        role=snowflake_roles.etl_service_account,
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_vendor}/{s3_prefix}/",
        copy_config=CopyConfigCsv(
            header_rows=1,
            timestamp_format="DD-MON-YYYY HH:MI:SS",
            date_format="DD-MON-YY",
        ),
    )
