import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake import SnowflakeAlertComplexEmailOperator
from include.config import email_lists, owners
from task_configs.dag_config.alerts_tableau_config import config as cfg

default_args = {
    "start_date": pendulum.datetime(2022, 4, 27, tz="America/Los_Angeles"),
    "retries": 0,
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_alerts_tableau_users",
    default_args=default_args,
    schedule="0 3 * * 1",
    catchup=False,
)

with dag:
    audit_ga = SnowflakeAlertComplexEmailOperator(
        task_id="tableau_users_alter",
        sql_or_path_list=cfg.sql_list,
        subject=cfg.subject,
        body_list=cfg.body_list,
        distribution_list=cfg.distribution_list,
    )
