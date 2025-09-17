import pendulum
from airflow import DAG

from edm.acquisition.configs import get_lake_history_tables
from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.dag_helpers import TFGControlOperator
from include.airflow.operators.snowflake_lake_hist import SnowflakeLakeHistoryOperator
from include.config import owners
from include.config.email_lists import data_integration_support

default_args = {
    'start_date': pendulum.datetime(2021, 4, 13, tz='America/Los_Angeles'),
    'retries': 0,
    'owner': owners.data_integrations,
    'email': data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id='edm_lake_history_ultra_cms',
    default_args=default_args,
    schedule='0 17 * * *',
    catchup=False,
    max_active_tasks=15,
    max_active_runs=1,
)


table_name_list = set(get_lake_history_tables(schema='ultra_cms'))

with dag:
    tfg_control = TFGControlOperator()
    for table_name in table_name_list:
        table = SnowflakeLakeHistoryOperator(table=table_name)
        tfg_control >> table
