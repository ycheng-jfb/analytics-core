from dataclasses import dataclass
from typing import Optional

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.genesys import GenesysEntitiesToS3Operator
from include.airflow.operators.snowflake_load import (
    Column,
    SnowflakeIncrementalLoadOperator,
)
from include.config import conn_ids, email_lists, owners, s3_buckets, stages
from include.utils.snowflake import CopyConfigCsv

default_args = {
    "start_date": pendulum.datetime(2019, 7, 13, 7, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.data_integrations,
    "email": email_lists.gms_support + email_lists.data_integration_support,
    "on_failure_callback": slack_failure_gsc,
}

dag = DAG(
    dag_id="global_apps_inbound_genesys_entities",
    default_args=default_args,
    schedule="0 1 * * *",
    catchup=False,
    max_active_tasks=20,
    max_active_runs=1,
    start_date=pendulum.datetime(2020, 1, 1, tz="America/Los_Angeles"),
)


@dataclass
class GenesysObjects:
    table_name: str
    column_list: list
    endpoint: str
    extra_params: Optional[dict] = None
    schema: str = "genesys"

    @property
    def s3_prefix(self):
        return f"lake/{self.schema}.{self.table_name}/v2"


objects_config = [
    GenesysObjects(
        table_name="genesys_user",
        column_list=[
            Column("user_id", "VARCHAR", uniqueness=True, source_name="id"),
            Column("user_name", "VARCHAR", source_name="name"),
            Column("department", "VARCHAR"),
            Column("state", "VARCHAR"),
            Column("title", "VARCHAR"),
            Column("user_login", "VARCHAR", source_name="username"),
            Column("manager", "VARIANT"),
            Column("locations", "VARIANT"),
            Column("groups", "VARIANT"),
            Column("employer_info", "VARIANT", source_name="employerInfo"),
            Column("updated_at", "TIMESTAMP_LTZ", delta_column=True),
        ],
        endpoint="users",
        extra_params={"expand": "groups,locations,employerInfo"},
    ),
    GenesysObjects(
        table_name="genesys_group",
        column_list=[
            Column("group_id", "VARCHAR", uniqueness=True, source_name="id"),
            Column("group_name", "VARCHAR", source_name="name"),
            Column("date_modified", "TIMESTAMP_LTZ", source_name="dateModified"),
            Column("state", "VARCHAR"),
            Column("updated_at", "TIMESTAMP_LTZ", delta_column=True),
        ],
        endpoint="groups",
        extra_params=None,
    ),
    GenesysObjects(
        table_name="genesys_location",
        column_list=[
            Column("location_id", "VARCHAR", uniqueness=True, source_name="id"),
            Column("location_name", "VARCHAR", source_name="name"),
            Column("state", "VARCHAR"),
            Column("updated_at", "TIMESTAMP_LTZ", delta_column=True),
        ],
        endpoint="locations",
        extra_params=None,
    ),
]

with dag:
    date_param = "{{ ts_nodash }}"
    for object in objects_config:
        to_snowflake = SnowflakeIncrementalLoadOperator(
            task_id=f"{object.table_name}_load_to_snowflake",
            table=object.table_name,
            schema=object.schema,
            database="lake",
            staging_database="lake_stg",
            snowflake_conn_id=conn_ids.Snowflake.default,
            column_list=object.column_list,
            files_path=f"{stages.tsos_da_int_inbound}/{object.s3_prefix}",
            copy_config=CopyConfigCsv(field_delimiter="\t", header_rows=0, skip_pct=5),
        )

        get_data = GenesysEntitiesToS3Operator(
            task_id=f"get_{object.table_name}_data",
            genesys_conn_id=conn_ids.Genesys.entities,
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            endpoint=object.endpoint,
            extra_params=object.extra_params,
            column_list=[x.source_name or x.name for x in object.column_list],
            bucket=s3_buckets.tsos_da_int_inbound,
            key=f"{object.s3_prefix}/{object.table_name}_{date_param}.csv.gz",
        )

        get_data >> to_snowflake
