from dataclasses import dataclass
from typing import Optional

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.snowflake_load import Column, SnowflakeIncrementalLoadOperator
from include.airflow.operators.zendesk import ZendeskToS3Operator
from include.config import conn_ids, email_lists, owners, s3_buckets, stages
from include.utils.snowflake import CopyConfigJson

default_args = {
    "start_date": pendulum.datetime(2019, 7, 13, 7, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.data_integrations,
    "email": email_lists.global_applications,
    "on_failure_callback": slack_failure_gsc,
}

dag = DAG(
    dag_id="global_apps_inbound_zendesk",
    default_args=default_args,
    schedule="0 1 * * *",
    catchup=False,
    max_active_tasks=20,
    max_active_runs=1,
    start_date=pendulum.datetime(2020, 1, 1, tz="America/Los_Angeles"),
)


@dataclass
class ZendeskObjects:
    table_name: str
    column_list: list
    endpoint: str
    extra_params: Optional[dict] = None
    is_incremental: bool = False
    use_cursor: bool = False

    SCHEMA = "gms"

    @property
    def s3_prefix(self):
        return f"lake/{self.SCHEMA}.{self.table_name}/v2"


objects_config = [
    ZendeskObjects(
        table_name="zendesk_tickets",
        column_list=[
            Column("id", "INT", uniqueness=True),
            Column("created_at", "TIMESTAMP_LTZ", delta_column=1),
            Column("updated_at", "TIMESTAMP_LTZ", delta_column=0),
            Column("type", "VARCHAR"),
            Column("subject", "VARCHAR"),
            Column("description", "VARCHAR"),
            Column("priority", "VARCHAR"),
            Column("status", "VARCHAR"),
            Column("recipient", "VARCHAR"),
            Column("requester_id", "INT"),
            Column("submitter_id", "INT"),
            Column("assignee_id", "INT"),
            Column("group_id", "INT"),
            Column("due_at", "TIMESTAMP_LTZ"),
            Column("satisfaction_rating", "VARCHAR"),
            Column("ticket_form_id", "INT"),
            Column("custom_fields", "VARIANT"),
            Column("brand_id", "INT"),
            Column("tags", "VARIANT"),
        ],
        endpoint="tickets",
        extra_params={"role[]": None},
        is_incremental=True,
        use_cursor=True,
    ),
    ZendeskObjects(
        table_name="zendesk_groups",
        column_list=[
            Column("id", "INT", uniqueness=True),
            Column("name", "VARCHAR"),
            Column("created_at", "TIMESTAMP_LTZ", delta_column=1),
            Column("updated_at", "TIMESTAMP_LTZ", delta_column=0),
        ],
        endpoint="groups",
    ),
    ZendeskObjects(
        table_name="zendesk_users",
        column_list=[
            Column("id", "INT", uniqueness=True),
            Column("name", "VARCHAR"),
            Column("email", "VARCHAR"),
            Column("role", "VARCHAR"),
            Column("created_at", "TIMESTAMP_LTZ", delta_column=1),
            Column("updated_at", "TIMESTAMP_LTZ", delta_column=0),
        ],
        endpoint="users",
        extra_params={"role[]": ['admin', 'end-user']},
        is_incremental=True,
    ),
    ZendeskObjects(
        table_name="zendesk_ticket_forms",
        column_list=[
            Column("id", "INT", uniqueness=True),
            Column("name", "VARCHAR"),
            Column("ticket_field_ids", "VARIANT"),
            Column("created_at", "TIMESTAMP_LTZ", delta_column=1),
            Column("updated_at", "TIMESTAMP_LTZ", delta_column=0),
        ],
        endpoint="ticket_forms",
    ),
    ZendeskObjects(
        table_name="zendesk_ticket_fields",
        column_list=[
            Column("id", "INT", uniqueness=True),
            Column("type", "VARCHAR"),
            Column("title", "VARCHAR"),
            Column("description", "VARCHAR"),
            Column("created_at", "TIMESTAMP_LTZ", delta_column=1),
            Column("updated_at", "TIMESTAMP_LTZ", delta_column=0),
        ],
        endpoint="ticket_fields",
    ),
    ZendeskObjects(
        table_name="zendesk_brands",
        column_list=[
            Column("id", "INT", uniqueness=True),
            Column("name", "VARCHAR"),
            Column("ticket_form_ids", "VARIANT"),
            Column("created_at", "TIMESTAMP_LTZ", delta_column=1),
            Column("updated_at", "TIMESTAMP_LTZ", delta_column=0),
        ],
        endpoint="brands",
    ),
]

with dag:
    date_param = "{{ ts_nodash }}"
    for object in objects_config:
        to_snowflake = SnowflakeIncrementalLoadOperator(
            task_id=f"{object.table_name}_load_to_snowflake",
            table=object.table_name,
            schema=object.SCHEMA,
            database="lake",
            staging_database="lake_stg",
            view_database='lake_view',
            snowflake_conn_id=conn_ids.Snowflake.default,
            column_list=object.column_list,
            files_path=f"{stages.tsos_da_int_inbound}/{object.s3_prefix}",
            copy_config=CopyConfigJson(),
            initial_load=True,
        )

        get_data = ZendeskToS3Operator(
            task_id=f"get_{object.table_name}_data",
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            endpoint=object.endpoint,
            extra_params=object.extra_params,
            use_cursor=object.use_cursor,
            initial_load_value='2018-01-01T00:00:00',
            is_incremental=object.is_incremental,
            column_list=[x.name for x in object.column_list],
            bucket=s3_buckets.tsos_da_int_inbound,
            key=f"{object.s3_prefix}/{object.table_name}_{date_param}.csv.gz",
            process_name=f'{object.table_name}',
            namespace='zendesk',
        )

        get_data >> to_snowflake
