import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.hermes import HermesTrackingEvents
from include.airflow.operators.snowflake_load import SnowflakeIncrementalLoadOperator
from include.config import (
    conn_ids,
    email_lists,
    owners,
    s3_buckets,
    snowflake_roles,
    stages,
)
from include.utils.snowflake import Column, CopyConfigJson

default_args = {
    "start_date": pendulum.datetime(2020, 12, 11, tz="America/Los_Angeles"),
    "retries": 0,
    "owner": owners.data_integrations,
    "email": email_lists.global_applications,
    "on_failure_callback": slack_failure_gsc,
}

dag = DAG(
    dag_id="global_apps_inbound_hermes",
    default_args=default_args,
    schedule="30 6,14 * * *",
    catchup=False,
)

column_list = [
    Column("tracking_id", "VARCHAR", uniqueness=True),
    Column("date_time", "TIMESTAMP_NTZ(3)", uniqueness=True),
    Column("location_latitude", "VARCHAR", uniqueness=True),
    Column("location_longitude", "VARCHAR", uniqueness=True),
    Column("links", "VARIANT", uniqueness=True),
    Column("tracking_point_description", "VARCHAR", uniqueness=True),
    Column("tracking_point_tracking_point_id", "VARCHAR", uniqueness=True),
    Column("updated_at", "TIMESTAMP_NTZ(3)", delta_column=True),
]

database = "lake"
schema = "hermes"
table = "tracking_events"
yr_mth = "{{macros.datetime.now().strftime('%Y%m')}}"
s3_prefix = f"lake/{database}.{schema}.{table}/v2"
biz_units = ["Fabletics", "JustFab", "Savage X"]

with dag:
    to_s3_list = []
    for bu in biz_units:
        bu_formatted = bu.replace(" ", "").lower()
        to_s3 = HermesTrackingEvents(
            task_id=f"to_s3_{bu_formatted}",
            bucket=s3_buckets.tsos_da_int_inbound,
            key=f"{s3_prefix}/{yr_mth}/{schema}_{table}_{bu_formatted}_{{{{ ts_nodash }}}}.json.gz",
            process_name=f"{database}.{schema}.{table}",
            namespace=f"hermes_tracking_events_{bu_formatted}",
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            biz_unit=bu,
            hook_conn_id=f"hermes_{bu_formatted}",
        )
        to_s3_list.append(to_s3)

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
        copy_config=CopyConfigJson(),
    )

    to_s3_list >> to_snowflake
