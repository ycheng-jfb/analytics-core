import pendulum
from include.config import email_lists, owners
from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake import SnowflakeProcedureOperator

from airflow import DAG

default_args = {
    "start_date": pendulum.datetime(2023, 1, 5, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    'on_failure_callback': slack_failure_edm,
}

dag = DAG(
    dag_id="edm_reporting_customer_communications_clicks",
    default_args=default_args,
    schedule="15 2 * * *",
    catchup=False,
)

with dag:
    customer_communications_proc = SnowflakeProcedureOperator(
        procedure='shared.marketing_channels_customer_communications.sql',
        database='reporting_prod',
        warehouse='DA_WH_ETL_LIGHT',
    )

    customer_clicks_proc = SnowflakeProcedureOperator(
        procedure='shared.marketing_channels_customer_clicks.sql',
        database='reporting_prod',
        warehouse='DA_WH_ETL_LIGHT',
    )
