import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.config import email_lists, owners

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2019, 1, 1, 7, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.global_apps_analytics,
    "email": email_lists.global_applications,
    "on_failure_callback": slack_failure_gsc,
}

dag = DAG(
    dag_id="global_apps_gsc_reporting_narvar_carrier_hourly",
    default_args=default_args,
    schedule="0 * * * *",
    catchup=False,
    max_active_tasks=4,
    max_active_runs=1,
    doc_md=__doc__,
)

with dag:
    gsc_narver_carrier_milestones = SnowflakeProcedureOperator(
        procedure="gsc.narvar_carrier_milestones.sql",
        database="reporting_prod",
        autocommit=False,
        warehouse="DA_WH_ETL_LIGHT",
    )
