import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.email_to_smb import EmailToSMBOperator
from include.airflow.operators.excel_smb import ExcelSMBToS3BatchOperator, SheetConfig
from include.airflow.operators.snowflake_load import SnowflakeIncrementalLoadOperator
from include.config import conn_ids, email_lists, owners, s3_buckets, stages
from include.utils.snowflake import Column, CopyConfigCsv

default_args = {
    "start_date": pendulum.datetime(2023, 5, 1, tz="America/Los_Angeles"),
    "retries": 2,
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_gsc,
}

dag = DAG(
    dag_id="global_apps_inbound_intertek",
    default_args=default_args,
    schedule="0 8 * * *",
    catchup=False,
    max_active_runs=1,
)

sheet_config = SheetConfig(
    sheet_name="TechStyle Custom Inherent Risk",
    schema="excel",
    table="intertek_inherent_risk",
    header_rows=1,
    column_list=[
        Column(
            name="supplier_program_relationship",
            type="VARCHAR",
            source_name="Supplier Program Relationship",
        ),
        Column(name="campaign", type="VARCHAR", source_name="Campaign"),
        Column(
            name="assignment_name",
            type="VARCHAR",
            source_name="Assignment: Assignment Name",
        ),
        Column(name="completed_date", type="DATE", source_name="Completed Date"),
        Column(
            name="numeric_score",
            type="NUMBER(38, 5)",
            source_name="Numeric Score",
        ),
        Column(
            name="result_color_name", type="VARCHAR", source_name="Result Color Name"
        ),
        Column(name="risk_source", type="VARCHAR", source_name="Risk Source"),
        Column(
            name="risk_ref_data_name", type="VARCHAR", source_name="Risk Ref Data: Name"
        ),
        Column(name="score", type="NUMBER", source_name="Score"),
        Column(name="supplier_name", type="VARCHAR", source_name="Supplier Name"),
    ],
    default_schema_version="v2",
)

with dag:
    to_smb = EmailToSMBOperator(
        task_id="inherent_risk.email_to_smb",
        remote_path="Inbound/airflow.intertek",
        smb_conn_id=conn_ids.SMB.nas01,
        from_address="@intertek.com",
        resource_address="inherentrisk@techstyle.com",
        share_name="BI",
        file_extensions=["xls", "xlsx"],
    )
    to_s3 = ExcelSMBToS3BatchOperator(
        task_id="inherent_risk.smb_to_s3",
        smb_dir="Inbound/airflow.intertek",
        share_name="BI",
        file_pattern_list=["*.xls*"],
        bucket=s3_buckets.tsos_da_int_inbound,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        smb_conn_id=conn_ids.SMB.nas01,
        is_archive_file=True,
        sheet_configs=[sheet_config],
        remove_header_new_lines=True,
        skip_downstream_if_no_files=True,
        default_schema_version="v2",
    )
    files_path = (
        f"{stages.tsos_da_int_inbound}/lake/{sheet_config.schema}.{sheet_config.table}/"
        f"{sheet_config.default_schema_version}/"
    )
    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="inherent_risk.s3_to_snowflake",
        files_path=files_path,
        database="lake",
        staging_database="lake_stg",
        view_database="lake_view",
        schema=sheet_config.schema,
        table=sheet_config.table,
        column_list=sheet_config.column_list,
        copy_config=CopyConfigCsv(
            field_delimiter="|",
            record_delimiter="\n",
            header_rows=sheet_config.header_rows,
            skip_pct=1,
        ),
    )
    to_smb >> to_s3 >> to_snowflake
