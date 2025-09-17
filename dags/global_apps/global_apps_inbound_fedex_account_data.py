import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.excel_smb import ExcelSMBToS3BatchOperator, SheetConfig
from include.airflow.operators.snowflake_load import (
    CopyConfigCsv,
    SnowflakeTruncateAndLoadOperator,
)
from include.config import conn_ids, email_lists, owners, s3_buckets, stages
from include.utils.snowflake import Column

default_args = {
    "start_date": pendulum.datetime(2021, 2, 20, 7, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="global_apps_inbound_fedex_account_data",
    default_args=default_args,
    schedule="0 9 * * *",
    catchup=False,
    max_active_tasks=20,
)

column_list = [
    Column("customer_account", "NUMBER", source_name="Customer Account"),
    Column("company_name", "VARCHAR", source_name="Company Name (Recommended)"),
    Column("department", "VARCHAR", source_name="Department (Recommended)"),
    Column("account_name", "VARCHAR", source_name="Account Name"),
    Column("account_owner", "VARCHAR", source_name="Account Owner"),
    Column("gl_code", "VARCHAR", source_name="GL Code"),
    Column("fdx01_meters", "VARCHAR", source_name="FDX01 Meters"),
    Column("fdx02_meters", "VARCHAR", source_name="FDX02 Meters"),
    Column("account_type", "VARCHAR", source_name="Account Type"),
    Column("account_status", "VARCHAR", source_name="Status"),
    Column("repurpose_date", "DATE", source_name="Repurpose Date"),
    Column("comments", "VARCHAR", source_name="Comments"),
]

sheets = {
    "Master_FedEx_Acct": SheetConfig(
        sheet_name="Master FedEx Acct ",
        schema="excel",
        table="fedex_account_data",
        s3_replace=False,
        header_rows=1,
        column_list=column_list,
    )
}

s3_key = "lake/excel.fedex_account_data/v1"

with dag:
    fedex_data_to_s3 = ExcelSMBToS3BatchOperator(
        task_id="fedex_data_to_s3",
        smb_dir="Inbound/airflow.fedex_account",
        share_name="BI",
        file_pattern_list=["*.xls*"],
        bucket=s3_buckets.tsos_da_int_inbound,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        smb_conn_id=conn_ids.SMB.nas01,
        is_archive_file=True,
        sheet_configs=[sheets["Master_FedEx_Acct"]],
        remove_header_new_lines=True,
        skip_downstream_if_no_files=True,
    )
    fedex_data_to_snowflake = SnowflakeTruncateAndLoadOperator(
        task_id="to_snowflake",
        database="lake",
        schema="excel",
        table="fedex_account_data",
        staging_database="lake_stg",
        view_database="lake_view",
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{s3_key}",
        copy_config=CopyConfigCsv(field_delimiter="|", header_rows=1),
    )

    fedex_data_to_s3 >> fedex_data_to_snowflake
