import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.config import email_lists, owners

default_args = {
    "start_date": pendulum.datetime(2023, 8, 14, tz="America/Los_Angeles"),
    'owner': owners.global_apps_analytics,
    'email': email_lists.global_applications,
    "on_failure_callback": slack_failure_gsc,
}

dag = DAG(
    dag_id='global_apps_ubt_reporting_monthly',
    default_args=default_args,
    schedule='0 10 1 * *',
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
)


with dag:
    ubt_sku_msrp_dataset = SnowflakeProcedureOperator(
        procedure='gsc.ubt_sku_msrp_dataset.sql', database='reporting_prod'
    )
