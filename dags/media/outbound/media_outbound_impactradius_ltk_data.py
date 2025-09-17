import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_media
from include.config import owners
from include.config.email_lists import airflow_media_support
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.impactradius import ImpactRadiusConversionsExportOperator

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2019, 4, 3, 7, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.media_analytics,
    "email": airflow_media_support,
    "on_failure_callback": slack_failure_media,
}

dag = DAG(
    dag_id="media_outbound_impactradius_ltk_data",
    default_args=default_args,
    schedule="0 13 * * *",
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
)

with dag:
    impact_radius_ltk_all_events = SnowflakeProcedureOperator(
        procedure="dbo.impact_radius_ltk_all_events.sql",
        database="reporting_media_base_prod",
        autocommit=False,
    )
    upload_task = ImpactRadiusConversionsExportOperator(
        task_id=f"upload_to_impactradius",
        initial_load_value="2024-11-04 09:00:00 +00:00",
        watermark_tables=["reporting_media_base_prod.dbo.impact_radius_ltk_all_events"],
    )
    impact_radius_ltk_all_events >> upload_task
