import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_media
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.config import owners
from include.config.email_lists import airflow_media_support

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2021, 10, 22, 8, tz="America/Los_Angeles"),
    "retries": 0,
    "owner": owners.wads,
    "email": airflow_media_support,
    "email_on_failure": True,
    "email_on_retry": True,
    "on_failure_callback": slack_failure_media,
}

dag = DAG(
    dag_id="web_analytic_transform_segment_api_volume",
    default_args=default_args,
    schedule="0 10 * * *",
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
)
with dag:
    sql_task = SnowflakeProcedureOperator(
        procedure="reporting.segment_api_volume.sql",
        database="segment",
        initial_load_value="2021-10-22 00:00:00 +00:00",
        watermark_tables=["segment.reporting.segment_api_volume"],
    )
