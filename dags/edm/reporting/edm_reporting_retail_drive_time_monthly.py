import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.config import email_lists, owners

default_args = {
    "start_date": pendulum.datetime(2022, 5, 1, 7, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
}


dag = DAG(
    dag_id="edm_reporting_retail_drive_time_monthly",
    default_args=default_args,
    schedule="0 2 1 * *",
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
)
with dag:
    retail_drive_time = SnowflakeProcedureOperator(
        procedure="retail.retail_drive_time.sql", database="reporting_prod"
    )
    sxf_retail_drive_time = SnowflakeProcedureOperator(
        procedure="sxf.retail_drive_time.sql", database="reporting_prod"
    )

    [retail_drive_time, sxf_retail_drive_time]
