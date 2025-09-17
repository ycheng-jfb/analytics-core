"""
Ingest cross border fee data
Login to svc_airflow_mailbox@techstyle.com like shared mail box to debug
"""

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.dag_helpers import chain_tasks
from include.airflow.operators.email_to_smb import EmailToSMBOperator
from include.airflow.operators.snowflake_load import SnowflakeTruncateAndLoadOperator
from include.config import email_lists
from include.utils.snowflake import Column

import pendulum
from airflow.models import DAG
from include.airflow.operators.excel_smb import ExcelSMBToS3Operator, SheetConfig

from include.utils.snowflake import CopyConfigCsv
from include.config import conn_ids, owners, s3_buckets, stages

from include.airflow.operators.dagrun_operator import TFGTriggerDagRunOperator

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2020, 10, 28, 0, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_inbound_cross_border_fee",
    default_args=default_args,
    schedule="0 8 * * *",
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
    doc_md=__doc__,
)
all_brands = [
    "PERSONAL RETAILING",
    "JUSTFAB",
    "LAVENDER",
    "YITTY",
    "FABLETICS",
]
sheets = {
    brand: SheetConfig(
        sheet_name="Page 1",
        schema="excel",
        table=f"entry_list_report_{brand.replace(' ', '').lower()}",
        header_rows=5,
        footer_rows=9,
        column_list=[
            Column("ENTRY", "VARCHAR(256)", uniqueness=True),
            Column("CUSTOMER_REFERENCE", "VARCHAR(256)"),
            Column("ENTRY_VAL", "NUMBER(38,10)"),
            Column("RELEASE_DATE", "DATE"),
            Column("GEN_DESC", "VARCHAR(256)"),
            Column("STMT_NO", "VARCHAR(256)"),
            Column("PMT_DATE", "VARCHAR(256)"),
            Column("DUTY", "NUMBER(38,10)"),
            Column("OTHER", "NUMBER(38,10)"),
            Column("TOTAL", "NUMBER(38,10)"),
            Column("STMT_MONTH", "VARCHAR(256)"),
        ],
    )
    for brand in all_brands
}

share_name = "BI"

with dag:
    trigger_gsc_reporting_cross_border_duty = TFGTriggerDagRunOperator(
        task_id="trigger_global_apps_gsc_reporting_cross_border_duty",
        trigger_dag_id="global_apps_gsc_reporting_cross_border_duty",
        execution_date="{{ data_interval_end }}",
    )
    for brand, config in sheets.items():
        email_to_smb_rcs = EmailToSMBOperator(
            task_id=f"email_to_smb_{config.table}",
            remote_path=f"/Inbound/airflow.cross_border_fee/{brand}",
            smb_conn_id=conn_ids.SMB.nas01,
            from_address="it.broker@g-global.com",
            resource_address="svc_airflow_mailbox@techstyle.com",
            share_name=share_name,
            file_extensions=["XLSX"],
            subjects=[f"DAILY ENTRY LIST REPORT {brand}"],
            add_timestamp_to_file=False,
        )
        to_s3 = ExcelSMBToS3Operator(
            task_id=f"excel_to_s3_{config.table}",
            smb_path=f"Inbound/airflow.cross_border_fee/{brand}/ENTRY LIST REPORT.XLSX",
            share_name=share_name,
            bucket=s3_buckets.tsos_da_int_inbound,
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            smb_conn_id=conn_ids.SMB.nas01,
            sheet_configs=[config],
            remove_header_new_lines=True,
            is_archive_file=True,
            archive_folder="archive",
        )
        to_snowflake = SnowflakeTruncateAndLoadOperator(
            task_id=f"{config.schema}.{config.table}.load_to_snowflake",
            files_path=f"{stages.tsos_da_int_inbound}/lake/{config.schema}.{config.table}",
            database="lake",
            staging_database="lake_stg",
            view_database="lake_view",
            schema="excel",
            table=f"{config.table}",
            column_list=config.column_list,
            initial_load=True,
            copy_config=CopyConfigCsv(
                field_delimiter="|",
                record_delimiter="\n",
                header_rows=1,
                skip_pct=1,
            ),
        )
        chain_tasks(
            email_to_smb_rcs,
            to_s3,
            to_snowflake,
            trigger_gsc_reporting_cross_border_duty,
        )
