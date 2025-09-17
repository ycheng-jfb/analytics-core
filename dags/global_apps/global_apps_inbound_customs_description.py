import pendulum
from airflow import DAG
from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.excel_smb import ExcelSMBToS3BatchOperator, SheetConfig
from include.airflow.operators.snowflake_load import SnowflakeIncrementalLoadOperator
from include.airflow.operators.snowflake_load import CopyConfigCsv
from include.airflow.operators.snowflake_to_mssql import SnowflakeToMsSqlBCPOperator
from include.config import (
    conn_ids,
    email_lists,
    owners,
    s3_buckets,
    stages,
    snowflake_roles,
)
from include.utils.snowflake import Column

default_args = {
    "start_date": pendulum.datetime(2024, 11, 15, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "email": email_lists.global_applications,
    "on_failure_callback": slack_failure_gsc,
    "retries": 0,
}

dag = DAG(
    dag_id="global_apps_inbound_customs_description",
    default_args=default_args,
    schedule="0 1 * * *",
    catchup=False,
)

sheet_config = SheetConfig(
    sheet_name="Sheet1",
    schema="excel",
    table="customs_description",
    s3_replace=False,
    header_rows=1,
    column_list=[
        Column("style", "VARCHAR", source_name="STYLE", uniqueness=True),
        Column("DESCRIPTION", "VARCHAR", source_name="DESCRIPTION"),
    ],
    default_schema_version="v1",
)


with dag:
    smb_to_s3 = ExcelSMBToS3BatchOperator(
        task_id="smb_to_s3",
        smb_dir="Inbound/airflow.gsc_customs_description",
        share_name="BI",
        file_pattern_list=["*.xls*"],
        bucket=s3_buckets.tsos_da_int_inbound,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        smb_conn_id=conn_ids.SMB.nas01,
        is_archive_file=True,
        sheet_configs=[sheet_config],
        remove_header_new_lines=True,
        skip_downstream_if_no_files=True,
        default_schema_version=sheet_config.default_schema_version,
    )

    files_path = (
        f"{stages.tsos_da_int_inbound}/lake/{sheet_config.schema}.{sheet_config.table}/"
        f"{sheet_config.default_schema_version}/"
    )

    s3_to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="s3_to_snowflake",
        database="lake",
        schema=sheet_config.schema,
        table=sheet_config.table,
        staging_database="lake_stg",
        snowflake_conn_id=conn_ids.Snowflake.default,
        role=snowflake_roles.etl_service_account,
        column_list=sheet_config.column_list,
        files_path=files_path,
        copy_config=CopyConfigCsv(
            field_delimiter="|",
            record_delimiter="\n",
            header_rows=sheet_config.header_rows,
            skip_pct=1,
        ),
    )

    snowflake_to_mssql = SnowflakeToMsSqlBCPOperator(
        task_id="snowflake_to_mssql",
        snowflake_database="lake_view",
        snowflake_schema="excel",
        snowflake_table="customs_description",
        mssql_target_table="customs_description",
        mssql_target_database="ssrs_reports",
        mssql_target_schema="wms",
        mssql_conn_id=conn_ids.MsSql.evolve01_app_airflow_rw,
        unique_columns=["style"],
        watermark_column="datetime_modified",
    )

    smb_to_s3 >> s3_to_snowflake >> snowflake_to_mssql
