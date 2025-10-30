from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.tableau import TableauRefreshOperator
from include.config import email_lists, owners

default_args = {
    "start_date": pendulum.datetime(2020, 5, 19, 7, tz="America/Los_Angeles"),
    'owner': owners.fl_analytics,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
}


def get_task_id(**kwargs):
    execution_date = kwargs['data_interval_end'].in_timezone('America/Los_Angeles')
    run_3_times_per_day = Variable.get("run_3_times_per_day")
    if execution_date.hour == 7:
        return [
            cash_management_deposit_log.task_id,
            cash_management_count_log.task_id,
            cash_management.task_id,
            fabletics_retail_dataset.task_id,
            friend_referrals_dataset.task_id,
            merch_master_item.task_id,
        ]
    elif run_3_times_per_day.lower() == 'true':
        return [merch_master_item.task_id]
    else:
        return []


def edm_acquisition_and_edw_execution_time(execution_date, context):
    run_time = context['data_interval_end'].in_timezone('America/Los_Angeles')
    if run_time.hour in (15, 23):
        return run_time - timedelta(hours=14)
    elif run_time.hour == 7:
        return run_time - timedelta(hours=13)
    else:
        return run_time


dag = DAG(
    dag_id='edm_reporting_fl_daily_7am',
    default_args=default_args,
    schedule='15 7,15,23 * * *',
    catchup=False,
    max_active_tasks=100,
)
with dag:
    # TODO: We will uncomment this once the script is migrated and validated
    # omnicart_product_level = SnowflakeProcedureOperator(
    #     procedure="omnicart.omnicart_product_level.sql", database="reporting_prod"
    # )
    check_edm_execution = ExternalTaskSensor(
        task_id='check_execution_of_edm_acquisition_and_edw',
        external_dag_id='edm_acquisition_and_edw',
        execution_date_fn=edm_acquisition_and_edw_execution_time,
        allowed_states=['success'],
        failed_states=['failed'],
        check_existence=True,
        timeout=2 * 60 * 60,
        poke_interval=10 * 60,
        mode='reschedule',
    )
    check = BranchPythonOperator(
        task_id="check",
        python_callable=get_task_id,
    )
    cash_management_deposit_log = SnowflakeProcedureOperator(
        procedure="retail.cash_management_deposit_log.sql", database="reporting_prod"
    )
    cash_management_count_log = SnowflakeProcedureOperator(
        procedure="retail.cash_management_count_log.sql", database="reporting_prod"
    )
    cash_management = SnowflakeProcedureOperator(
        procedure="retail.cash_management.sql", database="reporting_prod"
    )
    fabletics_retail_dataset = SnowflakeProcedureOperator(
        procedure="retail.fabletics_retail_dataset.sql", database="reporting_prod"
    )

    friend_referrals_dataset = SnowflakeProcedureOperator(
        procedure='membership.friend_referrals_dataset.sql', database='reporting_prod'
    )
    friend_referrals_tableau_refresh = TableauRefreshOperator(
        task_id='tableau_refresh_friend_referrals',
        data_source_name='Friend Referrals - EDW DataModel',
    )
    merch_master_item = SnowflakeProcedureOperator(
        procedure='fabletics.merch_master_item.sql', database='reporting_prod'
    )
    fl015_merch_master_dash_items_tableau_refresh = TableauRefreshOperator(
        task_id='tableau_refresh_fl015_merch_master_dash_items',
        data_source_name='FL015 Merch Master Dash Items',
    )
    (
        check_edm_execution
        >> check
        >> [
            cash_management_deposit_log,
            cash_management_count_log,
            cash_management,
            fabletics_retail_dataset,
            friend_referrals_dataset,
            merch_master_item,
        ]
    )

    friend_referrals_dataset >> friend_referrals_tableau_refresh
    merch_master_item >> fl015_merch_master_dash_items_tableau_refresh
