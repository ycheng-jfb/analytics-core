from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.dag_helpers import chain_tasks
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.tableau import TableauRefreshOperator
from include.config import email_lists, owners

default_args = {
    "start_date": pendulum.datetime(2020, 5, 19, 7, tz="America/Los_Angeles"),
    'owner': owners.analytics_engineering,
    "email": email_lists.edw_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id='edm_reporting_payment_dataset',
    default_args=default_args,
    schedule='0 5 * * *',
    catchup=False,
    max_active_runs=1,
)
with dag:
    check_edw_run = ExternalTaskSensor(
        task_id='check_edw_run_completion',
        external_dag_id='edm_acquisition_and_edw',
        external_task_id='await_edm_edw_load',
        execution_delta=timedelta(minutes=-795),
        timeout=7200,
        poke_interval=60 * 10,
        mode='reschedule',
        allowed_states=['success'],
    )

    check_cltv_monthly_run = ExternalTaskSensor(
        task_id='check_cltv_monthly_run_completion',
        external_dag_id='edm_analytics_base_customer_lifetime_value_monthly',
        external_task_id='edw_prod.analytics_base.customer_lifetime_value_monthly.sql',
        execution_delta=timedelta(minutes=-1215),
        timeout=7200,
        poke_interval=60 * 10,
        mode='reschedule',
        allowed_states=['success'],
    )

    order_transactions_by_reason = SnowflakeProcedureOperator(
        procedure='reporting.order_transactions_by_reason.sql',
        database='edw_prod',
    )

    payment_dataset_final_output = SnowflakeProcedureOperator(
        procedure='reporting.payment_dataset_final_output.sql',
        database='edw_prod',
    )

    action_from_billable_vips = SnowflakeProcedureOperator(
        procedure='shared.action_from_billable_vips.sql',
        database='reporting_base_prod',
    )

    customer_failed_billings_monthly = SnowflakeProcedureOperator(
        procedure='analytics_base.customer_failed_billings_monthly.sql',
        database='edw_prod',
    )

    payment_transactions_by_reason_tableau_refresh = TableauRefreshOperator(
        task_id='tableau_refresh_payment_transactions_by_reason',
        data_source_name='TFG028 - Payment Transactions by Reason - Datasource',
    )

    payment_dataset_tableau_refresh = TableauRefreshOperator(
        task_id='tableau_refresh_payment_dataset',
        data_source_name='TFG029 - Payment Dataset Final Output',
    )

    credit_card_updater_tableau_refresh = TableauRefreshOperator(
        task_id='tableau_refresh_credit_card_updater',
        data_source_name='TFG030 - Credit Card Updater - Datasource',
    )

    failed_billings_final_output_tableau_refresh = TableauRefreshOperator(
        task_id='tableau_refresh_failed_billings_final_output',
        data_source_name='TFG031 - Failed Billings Final Output',
    )

    bop_vip_customer_analysis_tableau_refresh = TableauRefreshOperator(
        task_id='tableau_refresh_bop_vip_customer_analysis',
        data_source_name='TFG034 - BOP VIP Customer Analysis',
    )

    action_from_billable_vips_tableau_refresh = TableauRefreshOperator(
        task_id='tableau_refresh_action_from_billable_vips',
        data_source_name='TFG039 - Action From Billable VIPs',
    )

    chain_tasks(
        check_edw_run,
        order_transactions_by_reason,
        payment_transactions_by_reason_tableau_refresh,
    )
    chain_tasks(
        check_edw_run,
        payment_dataset_final_output,
        [payment_dataset_tableau_refresh, credit_card_updater_tableau_refresh],
    )
    chain_tasks(
        check_edw_run,
        action_from_billable_vips,
        action_from_billable_vips_tableau_refresh,
    )
    chain_tasks(
        check_cltv_monthly_run,
        [customer_failed_billings_monthly, bop_vip_customer_analysis_tableau_refresh],
        failed_billings_final_output_tableau_refresh,
    )
