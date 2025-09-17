from datetime import timedelta

import pendulum
from airflow.operators.python import BranchPythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from include.airflow.callbacks.slack import slack_failure_p1_edw, slack_sla_miss_edw_p1
from include.airflow.dag_helpers import chain_tasks
from include.airflow.operators.dagrun_operator import TFGTriggerDagRunOperator
from include.airflow.operators.snowflake import SnowflakeEdwProcedureOperator
from include.config import owners
from include.config.email_lists import engineering_support

from airflow import DAG

default_args = {
    'start_date': pendulum.datetime(2019, 11, 19, tz='America/Los_Angeles'),
    'retries': 0,
    'owner': owners.data_integrations,
    'email': engineering_support,
    "on_failure_callback": slack_failure_p1_edw,
    "sla": timedelta(hours=3),
}

dag = DAG(
    dag_id='edm_acquisition_and_edw',
    default_args=default_args,
    schedule='15 1,9,18 * * *',
    catchup=False,
    max_active_tasks=1000,
    max_active_runs=1,
    sla_miss_callback=slack_sla_miss_edw_p1,
)


def get_next_execution_date(execution_date, context):
    return context['data_interval_end']


def trigger_dag(dag_id):
    return TFGTriggerDagRunOperator(
        task_id=f"trigger_{dag_id}",
        trigger_dag_id=dag_id,
        execution_date='{{ data_interval_end }}',
        skip_downstream_if_paused=True,
    )


def await_dag(dag_id):
    return ExternalTaskSensor(
        task_id=f"await_{dag_id}",
        external_dag_id=dag_id,
        mode='reschedule',
        execution_date_fn=get_next_execution_date,
        poke_interval=60 * 5,
    )


def check_execution_time(**kwargs):
    execution_time = kwargs['data_interval_end'].in_timezone('America/Los_Angeles')
    if execution_time.hour in (1, 18):
        return trigger_reporting.task_id
    else:
        return []


def check_run_time(**kwargs):
    execution_time = kwargs['data_interval_end'].in_timezone('America/Los_Angeles')
    if execution_time.hour in (1, 18):
        return trigger_edw.task_id
    else:
        return trigger_edw.task_id


def edm_lake_consolidated_high_frequency_execution_time(execution_date, context):
    run_time = context['data_interval_end'].in_timezone('America/Los_Angeles')
    if run_time.hour in (1, 9):
        return run_time - timedelta(minutes=215)
    elif run_time.hour == 18:
        return run_time - timedelta(minutes=155)
    else:
        return run_time


acq_dags = ['edm_acquisition_edw_sources', 'edm_lake_consolidated_edw_sources']
reporting_dag = 'edm_reporting_p1'
edw_load_dag = 'edm_edw_load'
fso_validation_and_snapshot = 'edm_reporting_finance_sales_ops_validation_and_snapshot'

with dag:

    check_dag_run_high_frequency = ExternalTaskSensor(
        task_id='check_high_frequency_completion',
        external_dag_id='edm_lake_consolidated_high_frequency',
        external_task_id='consolidation_completion',
        execution_date_fn=edm_lake_consolidated_high_frequency_execution_time,
        check_existence=True,
        timeout=1200,
        poke_interval=60 * 5,
        mode='reschedule',
    )

    decide_reporting_execution = BranchPythonOperator(
        python_callable=check_execution_time, task_id='check_time_reporting'
    )
    trigger_edw = trigger_dag(edw_load_dag)

    await_edw = await_dag(edw_load_dag)

    trigger_reporting = trigger_dag(reporting_dag)

    skip_edw = BranchPythonOperator(python_callable=check_run_time, task_id='check_edw_skip_run')
    trigger_fso_validation_and_snapshot = trigger_dag(fso_validation_and_snapshot)

    finance_sales_ops_stg = SnowflakeEdwProcedureOperator(
        procedure='analytics_base.finance_sales_ops_stg.sql',
        database="edw_prod",
        warehouse='DA_WH_EDW',
        watermark_tables=[
            'edw_prod.stg.fact_order',
        ],
    )

    trigger_edw_data_validation_and_alert = TFGTriggerDagRunOperator(
        task_id='trigger_edw_data_validation_and_alert',
        trigger_dag_id='edw_data_validation_and_alert',
        execution_date='{{ data_interval_end }}',
    )
    trigger_order_line_ext = TFGTriggerDagRunOperator(
        task_id='trigger_edw_analytics_base_order_line_ext',
        trigger_dag_id='edm_analytics_base_order_line_ext',
    )

    trigger_edm_reporting_fole = TFGTriggerDagRunOperator(
        task_id='trigger_edm_reporting_fole',
        trigger_dag_id='edm_reporting_fole',
    )

    (
        await_edw
        >> [
            trigger_edw_data_validation_and_alert,
            trigger_order_line_ext,
        ]
    )

    for dag_id in acq_dags:
        trigger_acquisition = trigger_dag(dag_id)
        await_acquisition = await_dag(dag_id)
        chain_tasks(
            trigger_acquisition,
            [await_acquisition, check_dag_run_high_frequency],
            skip_edw,
            trigger_edw,
            await_edw,
            finance_sales_ops_stg,
            decide_reporting_execution,
            trigger_reporting,
            trigger_fso_validation_and_snapshot,
        )

    finance_sales_ops_stg >> trigger_edm_reporting_fole
