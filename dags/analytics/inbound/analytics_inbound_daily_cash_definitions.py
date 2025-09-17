import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.tableau import TableauRefreshOperator
from include.config import owners
from include.config.email_lists import edw_support

default_args = {
    "start_date": pendulum.datetime(2020, 1, 28, 7, tz="America/Los_Angeles"),
    "retries": 3,
    'owner': owners.data_integrations,
    "email": edw_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="analytics_inbound_daily_cash_definitions",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_tasks=4,
    max_active_runs=1,
)

with dag:
    # Refresh "Daily Cash Metric Definitions" tableau datasource
    tableau_daily_cash_metric_definitions = TableauRefreshOperator(
        task_id='trigger_tableau_daily_cash_metric_definitions',
        data_source_id="954cd220-d456-45bb-aaa2-1b85bac5fcf4",
    )
