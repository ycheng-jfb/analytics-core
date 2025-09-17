import warnings
from pathlib import Path
from typing import Any, Dict
from datetime import timedelta

import pendulum
from airflow import DAG

from edm.mart.configs import get_mart_operator
from include import DAGS_DIR
from include.airflow.callbacks.slack import slack_failure_p1_edw
from include.config import owners
from include.config.email_lists import edw_support
from include.airflow.operators.dagrun_operator import TFGTriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    'start_date': pendulum.datetime(2020, 4, 19, tz='America/Los_Angeles'),
    'retries': 3,
    'owner': owners.analytics_engineering,
    'email': edw_support,
    "on_failure_callback": slack_failure_p1_edw,
}

dag = DAG(
    dag_id='edm_edw_load_fact_inventory',
    default_args=default_args,
    schedule='15 3 * * *',
    catchup=False,
    max_active_tasks=18,
    max_active_runs=1,
)

mart_operators: Dict[str, Any] = {}


def load_operator(table_name):
    if table_name not in mart_operators:
        mart_operators[table_name] = get_mart_operator(table_name)
    return mart_operators[table_name]


def check_run_time(**kwargs):
    if 'manual' in kwargs['run_id']:
        return []
    else:
        return check_edw_load_execution.task_id


table_config = {
    'edw_prod.stg.fact_inventory': [
        'edw_prod.stg.dim_warehouse',
        'edw_prod.reference.gfc_inventory_cost',
    ],
    'edw_prod.stg.fact_inventory_history': [
        'edw_prod.stg.fact_inventory',
        'edw_prod.reference.dropship_inventory_log',
    ],
    'edw_prod.stg.fact_inventory_system_date_history': [
        'edw_prod.stg.fact_inventory',
        'edw_prod.reference.dropship_inventory_log',
    ],
}
with dag:
    dummy = EmptyOperator(task_id="dummy")

    check_edw_load_execution = ExternalTaskSensor(
        task_id='check_edw_load_execution',
        external_dag_id='edm_edw_load',
        execution_delta=timedelta(minutes=-1320),
        check_existence=True,
        timeout=1200,
        poke_interval=60 * 5,
        mode='reschedule',
    )

    skip_trigger_merch_planning = BranchPythonOperator(
        python_callable=check_run_time, task_id='skip_trigger_merch_planning'
    )

    for table_name, dep_list in table_config.items():
        for dep_table_name in dep_list:
            dep_op = load_operator(dep_table_name)
            target_op = load_operator(table_name)
            dep_op >> target_op >> dummy

    trigger_merch_planning = TFGTriggerDagRunOperator(
        task_id='trigger_merch_planning',
        trigger_dag_id='edm_reporting_merch_planning',
        execution_date='{{ data_interval_end }}',
    )

    dummy >> skip_trigger_merch_planning >> check_edw_load_execution >> trigger_merch_planning
