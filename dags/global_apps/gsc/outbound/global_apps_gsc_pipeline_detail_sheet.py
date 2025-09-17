import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.snowflake_export import SnowflakeToSMBExcelOperator
from include.config import conn_ids, email_lists, owners

default_args = {
    "start_date": pendulum.datetime(2020, 10, 1, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.global_apps_analytics,
    "email": email_lists.global_applications,
    "on_failure_callback": slack_failure_gsc,
}


dag = DAG(
    dag_id="global_apps_gsc_pipeline_detail_sheet",
    default_args=default_args,
    schedule="21 */3 * * *",
    catchup=False,
    max_active_tasks=100,
    max_active_runs=1,
)

with dag:
    date_param = (
        "{{macros.tfgdt.to_pst(macros.datetime.now()).strftime('%Y%m%d-%H%M%S')}}"
    )

    to_file = SnowflakeToSMBExcelOperator(
        task_id="global_apps_export_gsc.pipeline_detail_sheet",
        sql_cmd="SELECT * FROM reporting_base_prod.gsc.pipeline_detail_sheet",
        remote_path=f"Outbound/gsc.pipeline_detail_sheet/GSC_PIPELINE_DATA_{date_param}_snowflake.xlsx",
        share_name="BI",
        smb_conn_id=conn_ids.SMB.nas01,
        header=True,
    )
