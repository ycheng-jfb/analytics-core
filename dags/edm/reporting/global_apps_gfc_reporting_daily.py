import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.dag_helpers import chain_tasks
from include.airflow.operators.snowflake import SnowflakeProcedureOperator

from include.airflow.operators.tableau import TableauRefreshOperator
from include.config import email_lists, owners

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2024, 5, 20, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.global_apps_analytics,
    "email": email_lists.global_applications,
    "on_failure_callback": slack_failure_gsc,
}


dag = DAG(
    dag_id="global_apps_gfc_reporting_daily",
    default_args=default_args,
    description="Twice Daily Global Apps GFC Reporting DAG",
    schedule_interval="0 */6 * * *",  # Run every 6 hours aka 4 times a day
    catchup=False,
    max_active_tasks=2,
    max_active_runs=1,
)

with dag:
    fulfillment_to_carrier = SnowflakeProcedureOperator(
        procedure="gfc.fulfillment_to_carrier_dataset.sql",
        database="reporting_prod",
    )
    fulfillment_to_carrier_refresh = TableauRefreshOperator(
        task_id="trigger_fulfillment_to_carrier_tableau_extract",
        data_source_id="0e57a47e-865a-4fef-b2b0-ce32b325d8d5",
    )
    fulfillment_to_carrier_agg_refresh = TableauRefreshOperator(
        task_id="trigger_fulfillment_to_carrier_agg_tableau_extract",
        data_source_id="7a1582fd-8d26-4bae-8968-e556e9ef79d2",
    )
    chain_tasks(
        fulfillment_to_carrier,
        fulfillment_to_carrier_refresh,
        fulfillment_to_carrier_agg_refresh,
    )
