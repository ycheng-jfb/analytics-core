from airflow.models import DAG
import pendulum
from include.config import conn_ids, owners
from include.airflow.callbacks.slack import slack_failure_gsc
from include.config.email_lists import data_integration_support
from include.airflow.operators.smb_to_sftp import SMBToSFTPOperator

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2024, 10, 1, 7, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.data_integrations,
    "email": data_integration_support,
    "on_failure_callback": slack_failure_gsc,
}

dag = DAG(
    dag_id="global_apps_outbound_purolator",
    default_args=default_args,
    schedule="0 21 * * *",
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
)

# Template-based filename with YYYY_MM_DD pattern
file_name_template = "Purolator_Returns_Report_{{ ds.split('-')[0] }}_{{ ds.split('-')[1] | int }}_{{ ds.split('-')[2] | int }}.xlsx"

# Template-based SMB path
smb_path_template = f"Outbound/gfc.purolator_returns_report/{file_name_template}"

# Target directory
sftp_target_dir = "/incoming"

with dag:
    smb_to_sftp = SMBToSFTPOperator(
        task_id="smb_to_sftp",
        remote_path=smb_path_template,  # Template for SMB path
        target_dir=sftp_target_dir,
        filename=file_name_template,  # Template for filename
        share_name="BI",
        smb_conn_id=conn_ids.SMB.nas01,
        sftp_conn_id="sftp_purolator",
    )
