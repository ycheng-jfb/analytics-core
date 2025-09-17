import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.s3 import BaseKeyMapper, MoveS3FilesOperator
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
    "start_date": pendulum.datetime(2021, 10, 15, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_inbound_sorting_hat",
    default_args=default_args,
    schedule="55 * * * *",
    catchup=False,
    max_active_tasks=1,
)


class KeyMapper(BaseKeyMapper):
    @property
    def target_key(self):
        return (
            f"processed/{self.filename[0:4]}_{self.filename[4:6]}_"
            f"{self.filename[6:8]}/{self.filename}"
        )


column_list = [Column("test_object", "VARIANT")]

database = "lake"
schema = "experiments"
table = "sorting_hat"

# TODO: Add year_month value to from path once we settle on pattern for using macros
custom_select = f"""
        SELECT
            TRIM($1, '"')
        FROM '{stages.tsos_da_int_sortinghat}/processed/'
"""


with dag:
    move_s3 = MoveS3FilesOperator(
        task_id="move_s3",
        source_bucket=s3_buckets.tsos_da_int_sortinghat,
        source_prefix="2025",
        target_bucket=s3_buckets.tsos_da_int_sortinghat,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        key_mapper=KeyMapper,
    )
    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="to_snowflake",
        database=database,
        schema=schema,
        table=table,
        staging_database="lake_stg",
        snowflake_conn_id=conn_ids.Snowflake.default,
        role=snowflake_roles.etl_service_account,
        column_list=column_list,
        files_path=None,
        copy_config=CopyConfigJson(match_by_column_name=False),
        custom_select=custom_select,
    )
    move_s3 >> to_snowflake
