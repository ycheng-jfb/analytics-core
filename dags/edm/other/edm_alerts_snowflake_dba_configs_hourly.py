import pendulum
from airflow.models import DAG
from include.config import owners, conn_ids
from include.config.email_lists import data_integration_support
from include import SQL_DIR
from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake import (
    SnowflakeAlertOperator,
    SnowflakeProcedureOperator,
)
from pathlib import Path

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2019, 1, 1, 7, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.data_integrations,
    "email": data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_alerts_snowflake_dba_configs_hourly",
    default_args=default_args,
    schedule="5 1,7,13,17 * * *",
    catchup=False,
    max_active_tasks=100,
    max_active_runs=1,
)


with dag:
    snowflake_unsuccessful_login_attempt_alert = SnowflakeAlertOperator(
        task_id="snowflake_unsuccessful_login_attempt_alert",
        distribution_list=[
            "rpoornima@techstyle.com",
            "savangala@fabletics.com",
            "rtanneeru@techstyle.com",
            "mhernick@techstyle.com",
        ],
        database="SNOWFLAKE",
        subject="Alert: Snowflake unsuccessful login attempt",
        slack_conn_id=conn_ids.SlackAlert.slack_default,
        slack_channel_name="airflow-alerts-edm",
        body="Snowflake unsuccessful login attempt:",
        alert_type=["mail", "slack"],
        sql_or_path=Path(
            SQL_DIR, "util", "procedures", "public.alert_for_unsuccessful_logins.sql"
        ),
    )
