import pendulum
from airflow.models import DAG
from datetime import timedelta


from include.airflow.callbacks.slack import slack_failure_media, slack_sla_miss_media
from include.airflow.operators.snowflake_load import (
    Column,
    CopyConfigCsv,
    SnowflakeIncrementalLoadOperator,
)
from include.config import email_lists, owners, stages

column_list = [
    Column("date", "DATE", uniqueness=True),
    Column("gift", "VARCHAR", uniqueness=True),
    Column("spend", "VARCHAR"),
    Column("selections", "INT"),
    Column("conversions", "INT"),
]

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2024, 12, 10, 7, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.media_analytics,
    "email": email_lists.airflow_media_support,
    "on_failure_callback": slack_failure_media,
    "sla": timedelta(hours=1),
    "execution_timeout": timedelta(hours=1),
}


dag = DAG(
    dag_id="media_inbound_nift_daily_spend",
    default_args=default_args,
    schedule="25 5 * * *",
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=slack_sla_miss_media,
)

with dag:
    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="load_to_snowflake",
        database="lake",
        schema="nift",
        table="daily_spend",
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_vendor}/inbound/svc_nift/",
        copy_config=CopyConfigCsv(field_delimiter=",", header_rows=1, skip_pct=1),
    )
