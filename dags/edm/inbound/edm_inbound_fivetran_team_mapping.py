from datetime import timedelta

import pendulum
from airflow.models import DAG
from include.airflow.operators.snowflake import SnowflakeAlertOperator

from include.airflow.callbacks.slack import slack_failure_media, slack_sla_miss_media
from include.airflow.operators.fivetran import FivetranUserMapOperator

from include.config import owners, conn_ids
from include.config.email_lists import airflow_media_support

default_args = {
    "start_date": pendulum.datetime(2024, 1, 1, 7, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.data_integrations,
    "email": airflow_media_support,
    "on_failure_callback": slack_failure_media,
    "sla": timedelta(hours=1),
    "execution_timeout": timedelta(hours=1),
}


dag = DAG(
    dag_id="edm_inbound_fivetran_team_mapping",
    default_args=default_args,
    schedule="0 20 * * *",
    catchup=False,
    max_active_tasks=7,
    max_active_runs=1,
    sla_miss_callback=slack_sla_miss_media,
)

with dag:
    sync_users = FivetranUserMapOperator(
        task_id=f"sync_users",
        request_params=None,
        path="",
    )
    unused_connectors = SnowflakeAlertOperator(
        task_id="alert_for_deleted_fivetran_schemas",
        sql_or_path="""
                          SELECT distinct catalog_name as database_name,schema_name,schema_owner,created,last_altered
                          FROM lake.information_schema.schemata s
                          left join lake_view.fivetran_log.connector c on lower(c.connector_name) = lower(s.schema_name)
                          where schema_owner = 'VENDOR_ROLE_FIVETRAN' and c._fivetran_deleted = TRUE
                     """,
        subject="Fivetran Schema's without active connector",
        body="Below are the Schema's without active connectors from fivetran",
        distribution_list=[],
        alert_type="slack",
        slack_conn_id=conn_ids.SlackAlert.slack_default,
        slack_channel_name="airflow-alerts-edm",
    )
    sync_users >> unused_connectors
