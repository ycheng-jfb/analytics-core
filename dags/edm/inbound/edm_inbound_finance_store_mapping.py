import pendulum
from airflow import DAG

from include.airflow.operators.snowflake_load import SnowflakeIncrementalLoadOperator
from include.config import owners, stages, conn_ids
from include.utils.snowflake import Column, CopyConfigCsv

finance_store_mapping_column_list = [
    Column("STORE_ID", "NUMBER(38,0)", uniqueness=True),
    Column("STORE_DESC", "VARCHAR(500)"),
    Column("LEGAL_ENTITY_NAME", "VARCHAR(255)"),
    Column("COMPANY_SEG", "NUMBER(38,0)"),
    Column("COMPANY_SEG_DESC", "VARCHAR(255)"),
    Column("BRAND_SEG", "VARCHAR(255)"),
    Column("BRAND_SEG_DESC", "VARCHAR(255)"),
    Column("REGION_SEG", "VARCHAR(255)"),
    Column("REGION_SEG_DESC", "VARCHAR(500)"),
]


default_args = {
    "owner": owners.analytics_engineering,
    "start_date": pendulum.datetime(2022, 4, 1, tz="America/Los_Angeles"),
}

dag = DAG(
    dag_id="edm_inbound_finance_store_mapping",
    default_args=default_args,
    catchup=False,
    schedule="10 9 * * *",
    max_active_tasks=2,
)

finance_store_mapping_s3_prefix = (
    "inbound/svc_oracle_ebs/lake.oracle_ebs.retail_store_mapping"
)

with dag:
    finance_store_mapping_to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="finance_store_mapping_to_snowflake",
        snowflake_conn_id=conn_ids.Snowflake.default,
        database="lake",
        schema="fpa",
        table="finance_store_mapping",
        column_list=finance_store_mapping_column_list,
        files_path=f"{stages.tsos_da_int_vendor}/{finance_store_mapping_s3_prefix}",
        copy_config=CopyConfigCsv(header_rows=1),
    )
