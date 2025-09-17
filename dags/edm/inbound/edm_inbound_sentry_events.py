from datetime import timedelta

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.sentry import SentryToS3Operator, SentryJsonToS3Operator
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.snowflake_load import (
    Column,
    SnowflakeIncrementalLoadOperator,
)
from include.config import email_lists, owners, s3_buckets, stages, conn_ids
from include.utils.snowflake import CopyConfigJson, CopyConfigCsv

schema = "sentry"

events_table = "events"
events_s3_prefix = f"lake/{schema}.{events_table}/daily_v1"
events_column_list = [
    Column("id", "VARCHAR", uniqueness=True),
    Column("project_name", "VARCHAR", source_name="project.name"),
    Column(
        "transaction",
        "VARCHAR",
    ),
    Column(
        "environment",
        "VARCHAR",
    ),
    Column("timestamp", "TIMESTAMP_NTZ"),
    Column("updated_at", "TIMESTAMP_LTZ", delta_column=True),
]

event_details_table = "event_details_raw"
event_details_s3_prefix = f"lake/{schema}.{event_details_table}/daily_v1"
event_details_column_list = [
    Column("id", "VARCHAR", uniqueness=True),
    Column(
        "groupID",
        "VARCHAR",
    ),
    Column(
        "eventID",
        "VARCHAR",
    ),
    Column(
        "projectID",
        "VARCHAR",
    ),
    Column(
        "size",
        "INT",
    ),
    Column(
        "entries",
        "VARIANT",
    ),
    Column(
        "dist",
        "VARCHAR",
    ),
    Column(
        "message",
        "VARCHAR",
    ),
    Column(
        "title",
        "VARCHAR",
    ),
    Column(
        "location",
        "VARCHAR",
    ),
    Column(
        "user",
        "VARCHAR",
    ),
    Column(
        "contexts",
        "VARIANT",
    ),
    Column(
        "sdk",
        "VARIANT",
    ),
    Column(
        "context",
        "VARIANT",
    ),
    Column(
        "packages",
        "VARIANT",
    ),
    Column(
        "type",
        "VARCHAR",
    ),
    Column(
        "metadata",
        "VARCHAR",
    ),
    Column(
        "tags",
        "VARIANT",
    ),
    Column(
        "platform",
        "VARCHAR",
    ),
    Column(
        "dateReceived",
        "VARCHAR",
    ),
    Column(
        "errors",
        "VARIANT",
    ),
    Column(
        "occurrence",
        "VARCHAR",
    ),
    Column(
        "_meta",
        "VARIANT",
    ),
    Column(
        "startTimestamp",
        "VARCHAR",
    ),
    Column(
        "endTimestamp",
        "VARCHAR",
    ),
    Column(
        "measurements",
        "VARCHAR",
    ),
    Column(
        "breakdowns",
        "VARCHAR",
    ),
    Column(
        "release",
        "VARCHAR",
    ),
    Column(
        "userReport",
        "VARCHAR",
    ),
    Column(
        "sdkUpdates",
        "VARIANT",
    ),
    Column(
        "resolvedWith",
        "VARIANT",
    ),
    Column(
        "nextEventID",
        "VARCHAR",
    ),
    Column(
        "previousEventID",
        "VARIANT",
    ),
]


default_args = {
    "start_date": pendulum.datetime(2025, 1, 1, 7, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
    "execution_timeout": timedelta(hours=2),
}

dag = DAG(
    dag_id=f"edm_inbound_sentry_events",
    default_args=default_args,
    schedule="0 */2 * * *",
    catchup=False,
    max_active_runs=1,
)

config = {
    "field": [
        "transaction",
        "timestamp",
        "environment",
    ],
    "statsPeriod": "12h",
}

with dag:
    date_param = "{{ data_interval_start.strftime('%Y%m%dT%H%M%S%z')[0:-3] }}"
    events_to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="sentry_events_to_snowflake",
        database="lake",
        schema=schema,
        table=events_table,
        column_list=events_column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{events_s3_prefix}",
        copy_config=CopyConfigCsv(field_delimiter="\t", header_rows=0, skip_pct=3),
    )
    get_events = SentryToS3Operator(
        task_id="sentry_events_to_s3",
        bucket=s3_buckets.tsos_da_int_inbound,
        key=f"{events_s3_prefix}/{schema}_{events_table}_{date_param}.tsv.gz",
        column_list=[x.source_name for x in events_column_list],
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        config=config,
    )

    event_details_to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="sentry_event_details_to_snowflake",
        database="lake",
        schema=schema,
        table=event_details_table,
        column_list=event_details_column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{event_details_s3_prefix}",
        copy_config=CopyConfigJson(),
    )
    get_event_details = SentryJsonToS3Operator(
        task_id="sentry_event_details_to_s3",
        initial_load_value="2025-02-06 05:30:36.755 -0800",
        bucket=s3_buckets.tsos_da_int_inbound,
        key=f"{event_details_s3_prefix}/{schema}_{event_details_table}_{date_param}.tsv.gz",
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        namespace="sentry",
        process_name="event_details",
    )
    flatten_events = SnowflakeProcedureOperator(
        database="lake",
        procedure="sentry.event_details.sql",
        watermark_tables=["lake.sentry.event_details_raw"],
    )
    (
        get_events
        >> events_to_snowflake
        >> get_event_details
        >> event_details_to_snowflake
        >> flatten_events
    )
