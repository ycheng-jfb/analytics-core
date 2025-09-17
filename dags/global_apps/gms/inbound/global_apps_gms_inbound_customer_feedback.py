import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.config import email_lists, owners
from task_configs.excel.global_applications.customer_feedback import (
    customer_feedback_data_source,
)

default_args = {
    "start_date": pendulum.datetime(2021, 5, 1, tz="America/Los_Angeles"),
    "retries": 2,
    "owner": owners.gms_analytics,
    "email": email_lists.global_applications_gms,
    "on_failure_callback": slack_failure_gsc,
}

dag = DAG(
    dag_id="global_apps_gms_inbound_customer_feedback",
    default_args=default_args,
    schedule="30 9 5,10 * *",  # check once
    catchup=False,
    max_active_tasks=100,
    max_active_runs=1,
)

with dag:
    for cfg in customer_feedback_data_source:
        to_s3 = cfg.to_s3
        to_snowflake = cfg.to_snowflake
        to_s3 >> to_snowflake
