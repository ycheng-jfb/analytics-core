import pendulum
from airflow.models import DAG
from include.config import owners, conn_ids
from include.config.email_lists import data_integration_support
from include import SQL_DIR
from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake import (
    SnowflakeAlertOperator,
    SnowflakeProcedureOperator,
    SnowflakeSqlOperator,
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
    dag_id="edm_alerts_snowflake_dba_configs_minutely",
    default_args=default_args,
    schedule="5,35 * * * *",
    catchup=False,
    max_active_tasks=100,
    max_active_runs=1,
)


with dag:
    apply_masking_policy = SnowflakeSqlOperator(
        task_id="util.public.apply_masking_policy",
        sql_or_path="CALL UTIL.PUBLIC.APPLY_PII_MASKING();",
        warehouse="DA_WH_ETL_LIGHT",
    )
    set_default_roles = SnowflakeProcedureOperator(
        database="util",
        procedure="public.set_default_user_config.sql",
        warehouse="DA_WH_ETL_LIGHT",
    )
