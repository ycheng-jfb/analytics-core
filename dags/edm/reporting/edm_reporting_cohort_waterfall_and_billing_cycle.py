import pendulum
from airflow.models import DAG
from airflow.operators.python import BranchPythonOperator
from include.config import email_lists, owners
from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.dag_helpers import chain_tasks
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.tableau import TableauRefreshOperator

default_args = {
    "start_date": pendulum.datetime(2022, 7, 25, tz="America/Los_Angeles"),
    'owner': owners.central_analytics,
    "email": email_lists.edw_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id='edm_reporting_cohort_waterfall_and_billing_cycle',
    default_args=default_args,
    schedule='0 7 1-6 * *',
    catchup=False,
    max_active_runs=1,
)


def check_date_and_time(**kwargs):
    execution_time = kwargs['data_interval_end'].in_timezone('America/Los_Angeles')
    if execution_time.hour == 7 and execution_time.day == 2:
        return [vip_level_gamers.task_id, billing_cycle.task_id]
    elif execution_time.hour == 7:
        return [billing_cycle.task_id]
    else:
        return []


with dag:
    billing_cycle = SnowflakeProcedureOperator(
        procedure='reporting.billing_cycle_rates_1st_through_5th.sql', database='edw_prod'
    )
    vip_level_gamers = SnowflakeProcedureOperator(
        procedure='reporting.vip_level_gamers.sql',
        database='edw_prod',
    )
    vip_level_gamers_tableau_refresh = TableauRefreshOperator(
        task_id='tableau_refresh_vip_level_gamers',
        data_source_name='TFG027 - VIP Level Gamers - Datasource',
    )
    check_run = BranchPythonOperator(
        python_callable=check_date_and_time, task_id='check_run_date_and_time'
    )
    chain_tasks(
        check_run,
        [billing_cycle, vip_level_gamers],
    )
    vip_level_gamers >> vip_level_gamers_tableau_refresh
