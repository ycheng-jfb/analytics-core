import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.smb_to_s3 import SMBToS3Operator
from include.airflow.operators.snowflake_load import SnowflakeIncrementalLoadOperator
from include.config import (
    conn_ids,
    email_lists,
    owners,
    s3_buckets,
    snowflake_roles,
    stages,
)
from include.utils.snowflake import Column, CopyConfigCsv

from include.airflow.operators.snowflake import SnowflakeProcedureOperator

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2020, 4, 1, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_operations_workday_employees",
    default_args=default_args,
    schedule="0 9 * * *",
    catchup=False,
)

column_list = [
    Column("employee_id", "VARCHAR", uniqueness=True),
    Column("first_name", "VARCHAR"),
    Column("last_name", "VARCHAR"),
    Column("preferred_first_name", "VARCHAR"),
    Column("preferred_last_name", "VARCHAR"),
    Column("business_title", "VARCHAR"),
    Column("worker_title", "VARCHAR"),
    Column("status", "VARCHAR"),
    Column("manager", "VARCHAR"),
    Column("company", "VARCHAR"),
    Column("brand", "VARCHAR"),
    Column("cost_center", "VARCHAR"),
    Column("email", "VARCHAR"),
    Column("start_date", "DATE", uniqueness=True),
    Column("termination_date", "DATE"),
    Column("location", "VARCHAR"),
    Column("MgmLevel01", "VARCHAR"),
    Column("MgmLevel02", "VARCHAR"),
    Column("MgmLevel03", "VARCHAR"),
    Column("MgmLevel04", "VARCHAR"),
    Column("MgmLevel05", "VARCHAR"),
    Column("MgmLevel06", "VARCHAR"),
    Column("MgmLevel07", "VARCHAR"),
    Column("updated_at", "TIMESTAMP_LTZ(3)", delta_column=0),
]

database = "lake"
schema = "workday"
table = "employees"
yr_mth = "{{macros.datetime.now().strftime('%Y%m')}}"
s3_prefix = f"lake/{database}.{schema}.{table}/v3/{yr_mth}"

custom_select = f"""
        SELECT
           f.$1, f.$2, f.$3, f.$4, f.$5, f.$6, f.$7, f.$8, f.$9, f.$10,f.$11,f.$12,
            IFNULL(f.$13, 'no email'),
             TO_DATE(f.$14, 'YYYY-MM-DD'),
            TO_DATE(f.$15, 'YYYY-MM-DD'),
            f.$16, f.$17, f.$18, f.$19, f.$20, f.$21, f.$22, f.$23,
            TO_TIMESTAMP(SPLIT_PART(SPLIT_PART(SPLIT_PART(metadata$filename, '/', 5), '.', 1),
                    '_', 3), 'YYYYMMDDTHH24MISS') AS updated_at
        FROM '{stages.tsos_da_int_inbound}/{s3_prefix}/' as f
"""

with dag:
    to_s3 = SMBToS3Operator(
        task_id="to_s3",
        remote_path="Inbound/airflow.workday/TechStyle_Snowflake_Data_Validation.csv",
        s3_bucket=s3_buckets.tsos_da_int_inbound,
        s3_key=f"{s3_prefix}/{schema}_{table}_{{{{ ts_nodash }}}}.csv.gz",
        share_name="BI",
        smb_conn_id=conn_ids.SMB.nas01,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        compression="gzip",
    )

    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="to_snowflake",
        database=database,
        schema=schema,
        table=table,
        staging_database="lake_stg",
        view_database="lake_view",
        snowflake_conn_id=conn_ids.Snowflake.default,
        role=snowflake_roles.etl_service_account,
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}/",
        copy_config=CopyConfigCsv(
            header_rows=1,
        ),
        custom_select=custom_select,
    )

    employee_history = SnowflakeProcedureOperator(
        procedure="workday.employee_history_last6months.sql",
        database="lake_history",
    )

    to_s3 >> to_snowflake >> employee_history
