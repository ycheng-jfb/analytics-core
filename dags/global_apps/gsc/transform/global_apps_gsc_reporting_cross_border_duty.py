import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.config import email_lists, owners

default_args = {
    "start_date": pendulum.datetime(2024, 8, 21, 0, tz="America/Los_Angeles"),
    "owner": owners.analytics_engineering,
    "email": email_lists.edw_engineering,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="global_apps_gsc_reporting_cross_border_duty",
    default_args=default_args,
    schedule="0 3 30 * *",
    catchup=False,
    max_active_tasks=8,
)

with dag:
    cross_border_fee_bulk_shipment = SnowflakeProcedureOperator(
        procedure="gsc.cross_border_fee_bulk_shipment.sql", database="reporting_prod"
    )
    cross_border_fee_ecomm = SnowflakeProcedureOperator(
        procedure="gsc.cross_border_fee_ecomm.sql", database="reporting_prod"
    )
    cross_border_fee_combined = SnowflakeProcedureOperator(
        procedure="gsc.cross_border_fee_combined.sql", database="reporting_prod"
    )

    [
        cross_border_fee_bulk_shipment,
        cross_border_fee_ecomm,
    ] >> cross_border_fee_combined
