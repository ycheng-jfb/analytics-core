import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.config import email_lists, owners

default_args = {
    "start_date": pendulum.datetime(2023, 8, 23, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="global_apps_gms_mismatch_address_monitoring",
    default_args=default_args,
    schedule="45 3,9,15 * * *",
    catchup=False,
    max_active_runs=1,
)

with dag:
    mismatch_address_monitoring = SnowflakeProcedureOperator(
        procedure="gms.mismatch_address_monitoring_dataset.sql",
        database="reporting_prod",
    )
