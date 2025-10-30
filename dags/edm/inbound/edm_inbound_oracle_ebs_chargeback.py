import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.config import owners
from include.config.email_lists import data_integration_support

default_args = {
    'start_date': pendulum.datetime(2022, 10, 4, tz='America/Los_Angeles'),
    'retries': 0,
    'owner': owners.data_integrations,
    'email': data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id='edm_inbound_oracle_ebs_chargeback',
    default_args=default_args,
    schedule='0 1 * * *',
    catchup=False,
    max_active_tasks=1000,
    max_active_runs=1,
)

with dag:
    to_snowflake_us = SnowflakeProcedureOperator(
        procedure='oracle_ebs.chargeback_us.sql',
        database='lake',
    )

    to_snowflake_eu = SnowflakeProcedureOperator(
        procedure='oracle_ebs.chargeback_eu.sql',
        database='lake',
    )
    chargeback_snapshot = SnowflakeProcedureOperator(
        procedure='oracle_ebs.chargeback_snapshot.sql',
        database='lake',
    )

[
    to_snowflake_us,
    to_snowflake_eu,
] >> chargeback_snapshot
