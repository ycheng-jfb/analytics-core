from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

from include import SQL_DIR
from include.airflow.callbacks.slack import slack_failure_sxf_data_team
from include.airflow.dag_helpers import chain_tasks
from include.airflow.operators.snowflake import SnowflakeAlertOperator, SnowflakeProcedureOperator
from include.airflow.operators.tableau import TableauRefreshOperator
from include.config import email_lists, owners

default_args = {
    "start_date": pendulum.datetime(2020, 1, 1, 7, tz="America/Los_Angeles"),
    'owner': owners.sxf_analytics,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_sxf_data_team,
}

dag = DAG(
    dag_id='edm_reporting_sxf_processing',
    default_args=default_args,
    schedule='0 5 * * *',
    catchup=False,
    max_active_tasks=20,
    max_active_runs=1,
)


def check_11am_func(**kwargs):
    execution_time = kwargs['data_interval_end'].in_timezone('America/Los_Angeles')
    if execution_time.hour == 11:
        return [check_ltv.task_id, check_edw.task_id, crm_optimization_dataset.task_id]
    elif execution_time.hour == 5:
        return [crm_optimization_dataset.task_id]
    else:
        return []


with dag:
    crm_optimization_dataset = SnowflakeProcedureOperator(
        procedure='sxf.crm_optimization_dataset.sql',
        database='reporting_prod',
        autocommit=False,
    )
    crm_contribution = SnowflakeProcedureOperator(
        procedure='sxf.crm_contribution.sql',
        database='reporting_prod',
        autocommit=False,
    )
    crm_optimization_dataset_tableau = TableauRefreshOperator(
        task_id='tableau_refresh_crm_optimization_dataset',
        data_source_name='SXF CRM Optimization Dataset',
    )
    crm_contribution_tableau = TableauRefreshOperator(
        task_id='tableau_refresh_crm_contribution',
        data_source_name='SXF CRM Contribution',
    )
    check_11am_and_5am = BranchPythonOperator(
        python_callable=check_11am_func, task_id='check_execution_time_11am'
    )
    chain_tasks(
        check_11am_and_5am,
        crm_optimization_dataset,
        crm_contribution,
        crm_optimization_dataset_tableau,
        crm_contribution_tableau,
    )
    with TaskGroup(group_id='sxf_load') as tg:
        order_line_dataset = SnowflakeProcedureOperator(
            procedure='sxf.order_line_dataset.sql',
            database='reporting_prod',
            watermark_tables=[
                'edw_prod.data_model_sxf.fact_order',
                'edw_prod.data_model_sxf.fact_order_product_cost',
                'edw_prod.data_model_sxf.fact_order_line',
                'lake_consolidated_view.ultra_merchant.order_line_discount',
                'edw_prod.data_model_sxf.fact_credit_event',
                'edw_prod.data_model_sxf.fact_return_line',
            ],
        )

        pcvr_product_psource = SnowflakeProcedureOperator(
            procedure='sxf.pcvr_product_psource.sql',
            database='reporting_prod',
            warehouse="DA_WH_ETL_HEAVY",
        )
        psource_product_conversion = SnowflakeProcedureOperator(
            procedure='sxf.psource_product_conversion.sql',
            database='reporting_prod',
            warehouse="DA_WH_ETL_HEAVY",
        )
        pcvr_product_psource_reduced = SnowflakeProcedureOperator(
            procedure='sxf.pcvr_product_psource_reduced.sql',
            database='reporting_prod',
        )
        pcvr_dataset_tableau = TableauRefreshOperator(
            task_id='tableau_refresh_sxf_psource_product_conversion_dataset',
            data_source_name='SXF_PSOURCE_PRODUCT_CONVERSION',
        )
        pcvr_product_psource_reduced_tableau = TableauRefreshOperator(
            task_id='tableau_refresh_pcvr_product_psource_reduced',
            data_source_name='SXF_PCVR_PRODUCT_PSOURCE_REDUCED',
        )
        check_11am = BranchPythonOperator(
            python_callable=check_11am_func, task_id='check_execution_time_11am'
        )
        check_ltv = ExternalTaskSensor(
            task_id="await_cltv",
            external_dag_id='edm_analytics_base_customer_lifetime_value_monthly',
            external_task_id='edw_prod.analytics_base.customer_lifetime_value_ltd.sql',
            mode='reschedule',
            execution_delta=timedelta(minutes=225),
            poke_interval=60 * 10,
            timeout=9600,
        )
        check_edw = ExternalTaskSensor(
            task_id="await_edw",
            external_dag_id='edm_edw_load',
            mode='reschedule',
            execution_delta=timedelta(minutes=-255),
            poke_interval=60 * 10,
            timeout=3600,
        )
        daily_bop_eop_counts = SnowflakeProcedureOperator(
            procedure='sxf.daily_bop_eop_counts.sql', database='reporting_prod'
        )
        cohort_waterfall_customer = SnowflakeProcedureOperator(
            procedure='sxf.cohort_waterfall_customer.sql',
            database='reporting_prod',
            warehouse="DA_WH_ETL_HEAVY",
        )
        cohort_waterfall = SnowflakeProcedureOperator(
            procedure='sxf.cohort_waterfall.sql',
            database='reporting_prod',
            warehouse="DA_WH_ETL_HEAVY",
        )
        chain_tasks(
            check_11am,
            check_ltv,
            cohort_waterfall_customer,
            cohort_waterfall,
        )
        check_11am >> check_edw >> daily_bop_eop_counts

    sxf_max_date_slack_alert = SnowflakeAlertOperator(
        task_id='sxf_max_date_slack_alert',
        sql_or_path=Path(SQL_DIR, 'reporting_prod', 'procedures', 'validation.sxf_max_date.sql'),
        subject='SXF max_date slack alert',
        body='Below is the list of unrefreshed SXF tables',
        distribution_list=['sxf-data-team-intake-aaaafsjwvsjonwlklilp4qtfcm@techstyle.slack.com'],
        trigger_rule='none_failed',
    )

    tg >> sxf_max_date_slack_alert
    crm_contribution >> sxf_max_date_slack_alert
    pcvr_product_psource >> psource_product_conversion >> pcvr_dataset_tableau
    pcvr_product_psource >> pcvr_product_psource_reduced >> pcvr_product_psource_reduced_tableau
