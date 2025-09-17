import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.email_to_smb import EmailToSMBOperator
from include.airflow.operators.excel_smb import (
    ExcelSMBToS3UseColNamesOperator,
    SheetConfig,
)
from include.airflow.operators.snowflake_load import (
    CopyConfigCsv,
    SnowflakeIncrementalLoadOperator,
)
from include.config import conn_ids, email_lists, owners, s3_buckets, stages
from task_configs.dag_config.social_compliance_config import column_list

default_args = {
    "start_date": pendulum.datetime(2022, 7, 15, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "email": email_lists.global_applications,
    "on_failure_callback": slack_failure_gsc,
    "retries": 0,
}

dag = DAG(
    dag_id="global_apps_inbound_social_compliance",
    default_args=default_args,
    schedule="50 1 * * 5",
    catchup=False,
)

sheets = [
    SheetConfig(
        sheet_name="SQP-QS",
        schema="excel",
        table="social_compliance",
        s3_replace=False,
        header_rows=1,
        column_list=column_list,
        default_schema_version="v2",
    ),
    SheetConfig(
        sheet_name="WCA-QS",
        schema="excel",
        table="social_compliance",
        s3_replace=False,
        header_rows=1,
        column_list=column_list,
        default_schema_version="v2",
    ),
]

with dag:
    to_smb = EmailToSMBOperator(
        task_id="to_smb",
        remote_path="Inbound/airflow.social_compliance",
        share_name="BI",
        smb_conn_id=conn_ids.SMB.nas01,
        from_address="admin.gscc@intertek.com",
        resource_address="qa_socialcompliance@techstyle.com",
        file_extensions=["xls"],
    )
    to_s3 = ExcelSMBToS3UseColNamesOperator(
        task_id="to_s3",
        smb_dir="Inbound/airflow.social_compliance",
        share_name="BI",
        file_pattern_list=["*.xls*"],
        bucket=s3_buckets.tsos_da_int_inbound,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        smb_conn_id=conn_ids.SMB.nas01,
        sheet_configs=sheets,
        remove_header_new_lines=True,
        is_archive_file=True,
        default_schema_version="v2",
    )
    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="to_snowflake",
        database="lake",
        schema="excel",
        table="social_compliance",
        staging_database="lake_stg",
        view_database="lake_view",
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_inbound}/lake/excel.social_compliance/v2",
        copy_config=CopyConfigCsv(field_delimiter="|", header_rows=2),
    )

    to_smb >> to_s3 >> to_snowflake
