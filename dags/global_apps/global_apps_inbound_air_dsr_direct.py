import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.email_to_smb import EmailToSMBOperator
from include.airflow.operators.smb_to_s3 import SMBToS3BatchOperator
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.config import conn_ids, email_lists, owners, s3_buckets

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2020, 10, 28, 0, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.data_integrations,
    "email": email_lists.global_applications,
    "on_failure_callback": slack_failure_gsc,
}

dag = DAG(
    dag_id="global_apps_inbound_air_dsr_direct",
    default_args=default_args,
    schedule="30 12,13 * * *",
    catchup=False,
    max_active_tasks=4,
    max_active_runs=1,
    doc_md=__doc__,
)

share_name = "JustFab"
with dag:
    email_to_smb_rcs = EmailToSMBOperator(
        task_id="email_to_smb_rcs",
        remote_path="/Supply Chain/Ops/Air DSR Upload",
        smb_conn_id=conn_ids.SMB.app01,
        from_address="@rcslogistics.com",
        resource_address="air_dsr@techstyle.com",
        share_name=share_name,
        file_extensions=["csv"],
    )
    email_to_smb_efl = EmailToSMBOperator(
        task_id="email_to_smb_efl",
        remote_path="/Supply Chain/Ops/Air DSR Upload",
        smb_conn_id=conn_ids.SMB.app01,
        from_address="@efl.global",
        resource_address="air_dsr@techstyle.com",
        share_name=share_name,
        file_extensions=["csv"],
    )
    to_s3 = SMBToS3BatchOperator(
        task_id="smb_to_s3",
        remote_dir="/Supply Chain/Ops/Air DSR Upload",
        s3_bucket=s3_buckets.tsos_da_int_inbound,
        s3_prefix="lake/gsc.air_dsr/v2",
        share_name="JustFab",
        smb_conn_id=conn_ids.SMB.app01,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        compression="gzip",
        file_pattern_list=["EFL_AIR_DSR_*.csv", "RCS_AIR_DSR_*.csv"],
        archive_remote_files=True,
        archive_folder="processed",
    )

    to_snowflake = SnowflakeProcedureOperator(
        database="reporting_prod",
        procedure="gsc.air_dsr.sql",
        warehouse="DA_WH_ETL_LIGHT",
    )

    email_to_smb_rcs >> email_to_smb_efl >> to_s3 >> to_snowflake
