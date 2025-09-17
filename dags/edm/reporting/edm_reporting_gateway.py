import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.config import owners
from include.config.email_lists import airflow_media_support

default_args = {
    'depends_on_past': False,
    'start_date': pendulum.datetime(2021, 1, 25, 7, tz='America/Los_Angeles'),
    'retries': 1,
    'owner': owners.data_integrations,
    'email': airflow_media_support,
    'on_failure_callback': slack_failure_edm,
}

dag = DAG(
    dag_id='edm_reporting_gateway',
    default_args=default_args,
    schedule='0 5,11,20 * * *',
    catchup=False,
    max_active_tasks=100,
    max_active_runs=1,
)

with dag:
    dm_gateway = SnowflakeProcedureOperator(
        database='reporting_base_prod',
        procedure='history.dm_gateway.sql',
    )

    dim_gateway_test_site = SnowflakeProcedureOperator(
        database='reporting_base_prod',
        procedure='shared.dim_gateway_test_site.sql',
    )
