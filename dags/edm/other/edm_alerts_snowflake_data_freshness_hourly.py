from pathlib import Path

import pendulum
from airflow.models import DAG

from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake import SnowflakeAlertOperator
from include.config import conn_ids, owners
from include.config.email_lists import data_integration_support

from task_configs.dag_config.data_freshness_config import hvr_config_list, non_hvr_config_list

default_args = {
    'depends_on_past': False,
    'start_date': pendulum.datetime(2019, 1, 1, 7, tz='America/Los_Angeles'),
    'retries': 1,
    'owner': owners.data_integrations,
    'email': data_integration_support,
    'on_failure_callback': slack_failure_edm,
}

dag = DAG(
    dag_id='edm_alerts_snowflake_data_freshness_hourly',
    default_args=default_args,
    schedule='0,30 * * * *',
    catchup=False,
    max_active_tasks=100,
    max_active_runs=1,
)

with dag:
    data_freshness_parent = EmptyOperator(task_id='data_freshness_parent')

    # hvr
    with TaskGroup(group_id='hvr') as tg1:
        for cfg in hvr_config_list:
            alerts = SnowflakeAlertOperator(
                task_id=cfg.get_task_id(),
                sql_or_path=cfg.get_sql_query(),
                distribution_list=data_integration_support,
                database='snowflake',
                alert_type=['slack', 'mail'],
                slack_conn_id=conn_ids.SlackAlert.edm_p1,
                slack_channel_name="airflow-edm-p1",
                subject=cfg.get_email_subject(),
                body=cfg.get_email_body(),
            )
            data_freshness_parent >> alerts

    # for non-HVR, LAKE_CONSOLIDATED
    with TaskGroup(group_id='nonhvr') as tg2:
        for cfg in non_hvr_config_list:
            alerts = SnowflakeAlertOperator(
                task_id=cfg.get_task_id(),
                sql_or_path=cfg.get_sql_query(),
                distribution_list=data_integration_support,
                database='snowflake',
                alert_type=['slack', 'mail'],
                slack_conn_id=conn_ids.SlackAlert.edm_p1,
                slack_channel_name="airflow-edm-p1",
                subject=cfg.get_email_subject(),
                body=cfg.get_email_body(),
            )
            data_freshness_parent >> alerts
