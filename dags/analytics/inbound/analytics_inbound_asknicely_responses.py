from dataclasses import dataclass

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.asknicely_get_responses import (
    AsknicelyResponsesToS3Operator,
)
from include.airflow.operators.snowflake_load import SnowflakeIncrementalLoadOperator
from include.config import (
    conn_ids,
    email_lists,
    owners,
    s3_buckets,
    snowflake_roles,
    stages,
)
from include.utils.snowflake import Column, CopyConfigCsv

process_end_date = "{{macros.tfgdt.to_pst(macros.datetime.now()).date()}}"
process_start_date = (
    "{{macros.tfgdt.to_pst(macros.datetime.now()).date().add(days=-2)}}"
)


@dataclass
class Config:
    process_name: str

    @property
    def connection_id(self):
        return self.process_name

    @property
    def filename(self):
        return f"{self.process_name}_{str(process_start_date)}.csv.gz"


default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2020, 1, 1, 7, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "email": email_lists.analytics_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="analytics_inbound_asknicely_responses",
    default_args=default_args,
    schedule="0 2 * * *",
    catchup=False,
)

column_list = [
    Column("business_unit", "VARCHAR", source_name="business_unit"),
    Column("country", "VARCHAR", source_name="country"),
    Column("response_id", "INT", source_name="response_id", uniqueness=True),
    Column("response_date", "TIMESTAMP_NTZ", source_name="response_date"),
    Column("sent_date", "TIMESTAMP_NTZ", source_name="sent_date"),
    Column("comment", "VARCHAR", source_name="comment"),
    Column(
        "asknicely_responses_data", "VARCHAR", source_name="asknicely_responses_data"
    ),
    Column("updated_at", "TIMESTAMP_LTZ(3)", source_name="updated_at"),
]

config_list = [
    Config(process_name="ASKNICELY_JF"),
    Config(process_name="ASKNICELY_FK"),
    Config(process_name="ASKNICELY_FL"),
    Config(process_name="ASKNICELY_SD"),
]

database = "lake"
schema = "ask_nicely"
table = "responses_data"
s3_prefix = f"lake/{database}.{schema}.{table}/v3/" + str(process_start_date)

with dag:
    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="analytics_inbound_asknicely_responses_to_snowflake",
        snowflake_conn_id=conn_ids.Snowflake.default,
        database=database,
        schema=schema,
        table=table,
        staging_database="lake_stg",
        view_database="lake_view",
        column_list=column_list,
        copy_config=CopyConfigCsv(field_delimiter="\t", header_rows=1),
        files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}/",
        role=snowflake_roles.etl_service_account,
    )

    for cfg in config_list:
        to_s3 = AsknicelyResponsesToS3Operator(
            task_id="analytics_inbound_" + cfg.process_name + "_to_s3",
            asknicely_conn_id=cfg.connection_id,
            aws_conn_id=conn_ids.S3.tsos_da_int_prod,
            process_start_date=process_start_date,
            process_end_date=process_end_date,
            s3_bucket=s3_buckets.tsos_da_int_inbound,
            s3_key=f"{s3_prefix}/{cfg.filename}",
            file_column_list=[x.source_name for x in column_list],
            write_header=True,
        )

        to_s3 >> to_snowflake
