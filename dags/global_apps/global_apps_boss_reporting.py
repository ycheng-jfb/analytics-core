import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.dag_helpers import chain_tasks
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.config import email_lists, owners

default_args = {
    "start_date": pendulum.datetime(2021, 11, 9, 7, tz="America/Los_Angeles"),
    'owner': owners.gms_analytics,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_gsc,
}

dag = DAG(
    dag_id='global_apps_boss_reporting',
    default_args=default_args,
    schedule='0 16 * * *',
    catchup=False,
    max_active_tasks=8,
)

with dag:
    bos_boss_forecast = SnowflakeProcedureOperator(
        procedure='gms.bos_boss_forecast.sql', database='reporting_prod'
    )
    bos_raw_data = SnowflakeProcedureOperator(
        procedure='gms.bos_raw_data.sql', database='reporting_prod'
    )
    bos_joining_planned_actuals = SnowflakeProcedureOperator(
        procedure='gms.bos_joining_planned_actuals.sql', database='reporting_prod'
    )
    bos_raw_data_final = SnowflakeProcedureOperator(
        procedure='gms.bos_raw_data_final.sql', database='reporting_prod'
    )
    bos_raw_data_daily = SnowflakeProcedureOperator(
        procedure='gms.bos_raw_data_daily.sql', database='reporting_prod'
    )
    chain_tasks(
        bos_boss_forecast,
        bos_raw_data,
        bos_joining_planned_actuals,
        bos_raw_data_final,
        bos_raw_data_daily,
    )
