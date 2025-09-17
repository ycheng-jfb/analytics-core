import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.snowflake_load import (
    Column,
    SnowflakeIncrementalLoadOperator,
)
from include.airflow.operators.zendesk import ZendeskS3CsvOperator
from include.config import (
    conn_ids,
    email_lists,
    owners,
    s3_buckets,
    snowflake_roles,
    stages,
)
from include.utils.snowflake import CopyConfigCsv

default_args = {
    "start_date": pendulum.datetime(2021, 5, 26, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.data_integrations,
    "email": email_lists.global_applications,
    "on_failure_callback": slack_failure_gsc,
}

dag = DAG(
    dag_id="global_apps_inbound_zendesk_ticket_metrics",
    default_args=default_args,
    schedule="0 1 * * *",
    catchup=False,
)

column_list = [
    Column("agent_wait_time_in_minutes_business", "NUMBER"),
    Column("agent_wait_time_in_minutes_calendar", "NUMBER"),
    Column("assigned_at", "TIMESTAMP_NTZ(3)"),
    Column("assignee_stations", "NUMBER"),
    Column("assignee_updated_at", "TIMESTAMP_NTZ(3)"),
    Column("created_at", "TIMESTAMP_NTZ(3)"),
    Column("first_resolution_time_in_minutes_business", "NUMBER"),
    Column("first_resolution_time_in_minutes_calendar", "NUMBER"),
    Column("full_resolution_time_in_minutes_business", "NUMBER"),
    Column("full_resolution_time_in_minutes_calendar", "NUMBER"),
    Column("group_stations", "NUMBER"),
    Column("id", "NUMBER", uniqueness=True),
    Column("initially_assigned_at", "TIMESTAMP_NTZ(3)"),
    Column("latest_comment_added_at", "TIMESTAMP_NTZ(3)"),
    Column("on_hold_time_in_minutes_business", "NUMBER"),
    Column("on_hold_time_in_minutes_calendar", "NUMBER"),
    Column("reopens", "NUMBER"),
    Column("replies", "NUMBER"),
    Column("reply_time_in_minutes_business", "NUMBER"),
    Column("reply_time_in_minutes_calendar", "NUMBER"),
    Column("requester_updated_at", "TIMESTAMP_NTZ(3)"),
    Column("requester_wait_time_in_minutes_business", "NUMBER"),
    Column("requester_wait_time_in_minutes_calendar", "NUMBER"),
    Column("solved_at", "TIMESTAMP_NTZ(3)"),
    Column("status_updated_at", "TIMESTAMP_NTZ(3)"),
    Column("ticket_id", "NUMBER"),
    Column("updated_at", "TIMESTAMP_NTZ(3)"),
    Column("url", "VARCHAR"),
    Column("tfg_updated_at", "TIMESTAMP_NTZ(3)", delta_column=True),
]

database = "lake"
schema = "gms"
table = "zendesk_ticket_metrics"
yr_mth = "{{macros.datetime.now().strftime('%Y%m')}}"
s3_prefix = f"lake/{database}.{schema}.{table}/v2/{yr_mth}"

with dag:
    to_s3 = ZendeskS3CsvOperator(
        task_id="to_s3",
        zendesk_conn_id=conn_ids.Zendesk.default,
        endpoint="ticket_metrics",
        params={"page[size]": 100},
        bucket=s3_buckets.tsos_da_int_inbound,
        key=f"{s3_prefix}/{schema}_{table}_{{{{ ts_nodash }}}}.csv.gz",
        column_list=[x.name for x in column_list],
        write_header=True,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
    )

    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="to_snowflake",
        database=database,
        schema=schema,
        table=table,
        staging_database="lake_stg",
        view_database="lake_view",
        snowflake_conn_id=conn_ids.Snowflake.default,
        role=snowflake_roles.etl_service_account,
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}/",
        copy_config=CopyConfigCsv(field_delimiter="\t", header_rows=1),
    )

    to_s3 >> to_snowflake
