import time

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from include.airflow.callbacks.slack import slack_failure_edm
from include.config import owners

default_args = {
    "start_date": pendulum.datetime(2019, 11, 19, tz="America/Los_Angeles"),
    "retries": 0,
    "owner": owners.data_integrations,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="airflow_test_child_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
    concurrency=1000,
    max_active_runs=1,
)


def test_wait_func():
    time.sleep(180)


with dag:
    test_task = PythonOperator(task_id="test_wait_3min", python_callable=test_wait_func)
