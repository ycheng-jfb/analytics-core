from datetime import timedelta

import pendulum
from airflow.models import DAG
from include.config import conn_ids, owners
from include.config.email_lists import data_integration_support
from include.airflow.callbacks.slack import slack_failure_edm, slack_sla_miss_edm
from include.airflow.operators.mssql import MsSqlOperator
from include.airflow.operators.mssql_to_sftp import MssqlToSFTPOperator

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2024, 1, 1, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.data_integrations,
    "email": data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_outbound_rfid_inventory_export",
    default_args=default_args,
    schedule_interval="0 * * * *",
    catchup=False,
    max_active_tasks=4,
    max_active_runs=1,
    sla_miss_callback=slack_sla_miss_edm,
)

with dag:
    rfid_mssql = MsSqlOperator(
        sql="[analytic].[dbo].[usp_rfid_inventory_export]",
        task_id="usp_rfid_inventory_export",
        mssql_conn_id=conn_ids.MsSql.fabletics_app_airflow,
        sla=timedelta(minutes=25),
    )
    rfid_sftp = MssqlToSFTPOperator(
        task_id="rfid_sftp",
        sql_or_path="""select SITE_CODE,
            PRODUCT_ID,
            QTY,
            CAST(TIMESTAMP AS smalldatetime) AS TIMESTAMP,
            TRANSACTION_CODE,
            ZONE,
            EPC,
            POS_ID from analytic.dbo.rfid_export""",
        mssql_conn_id=conn_ids.MsSql.fabletics_app_airflow,
        sftp_conn_id=conn_ids.SFTP.sftp_truevuesps,
        remote_dir="/pos_file_upload",
        filename="rfid_export.csv",
        header=True,
        sla=timedelta(minutes=25),
    )
    rfid_mssql >> rfid_sftp
