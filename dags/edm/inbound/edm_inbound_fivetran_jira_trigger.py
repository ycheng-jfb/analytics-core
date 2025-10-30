from dataclasses import dataclass
from datetime import timedelta

import pendulum
import time
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from typing import Dict

from include.airflow.callbacks.slack import slack_failure_media, slack_sla_miss_media
from include.airflow.operators.fivetran import FivetranOperator
from include.airflow.operators.tableau import TableauRefreshOperator

from include.config import owners
from include.config.email_lists import airflow_media_support

default_args = {
    "start_date": pendulum.datetime(2024, 1, 1, 7, tz="America/Los_Angeles"),
    "retries": 1,
    'owner': owners.data_integrations,
    "email": airflow_media_support,
    "on_failure_callback": slack_failure_media,
    "sla": timedelta(hours=1),
    "execution_timeout": timedelta(hours=1),
}


def wait():
    print("waiting for fivetran sync to complete")
    time.sleep(600)


@dataclass
class Connectors:
    connector_name: str
    connector_id: str
    request_params: Dict
    method: str

    @property
    def connector_sync_path(self):
        return f"connectors/{self.connector_id}/sync"


connectors = [
    Connectors(
        connector_name='central_jira_cloud_v1',
        connector_id='interminable_wearily',
        request_params={"force": False},
        method='POST',
    )
]

dag = DAG(
    dag_id="edm_inbound_fivetran_jira_trigger",
    default_args=default_args,
    schedule="0,30 8-17 1-14 * *",
    catchup=False,
    max_active_tasks=7,
    max_active_runs=1,
    sla_miss_callback=slack_sla_miss_media,
)

with dag:
    jira_capex_tableau_refresh = TableauRefreshOperator(
        task_id='jira_capex_tableau_refresh',
        data_source_id='0ba9d19a-a847-4a91-add8-30c060430f47',
    )
    wait_for_sync = PythonOperator(task_id='wait_for_sync', python_callable=wait)
    for connector in connectors:
        sync_data = FivetranOperator(
            task_id=f"{connector.connector_name}_sync",
            request_params=connector.request_params,
            method=connector.method,
            path=connector.connector_sync_path,
        )
        sync_data >> wait_for_sync >> jira_capex_tableau_refresh
