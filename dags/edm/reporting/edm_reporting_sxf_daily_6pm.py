import pendulum
from airflow import DAG
from airflow.operators.python import BranchPythonOperator

from include.airflow.callbacks.slack import slack_failure_sxf_data_team
from include.airflow.dag_helpers import chain_tasks
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.config import email_lists, owners

default_args = {
    "start_date": pendulum.datetime(2022, 5, 1, 7, tz="America/Los_Angeles"),
    'owner': owners.sxf_analytics,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_sxf_data_team,
}

dag = DAG(
    dag_id='edm_reporting_sxf_daily_6pm',
    default_args=default_args,
    schedule='0 18,21 * * *',
    catchup=False,
    max_active_tasks=20,
    max_active_runs=1,
)


def check_time(**kwargs):
    execution_time = kwargs['data_interval_end'].in_timezone('America/Los_Angeles')
    if execution_time.hour == 18:
        return [custom_segment_waterfall.task_id]
    elif execution_time.hour == 21:
        return [custom_segment_waterfall_growth.task_id]
    else:
        return []


with dag:
    custom_segment_waterfall = SnowflakeProcedureOperator(
        procedure='sxf.custom_segment_waterfall.sql',
        database='reporting_prod',
        warehouse="DA_WH_ETL_HEAVY",
    )
    custom_segment_waterfall_growth = SnowflakeProcedureOperator(
        procedure='sxf.custom_segment_waterfall_growth.sql',
        database='reporting_prod',
        warehouse="DA_WH_ETL_HEAVY",
    )
    check_run = BranchPythonOperator(
        python_callable=check_time, provide_context=True, task_id='check_time'
    )
    chain_tasks(
        check_run,
        [custom_segment_waterfall, custom_segment_waterfall_growth],
    )
