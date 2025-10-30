import pendulum
from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.builder import BuilderHDYHToS3Operator
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.snowflake_load import CopyConfigCsv, SnowflakeIncrementalLoadOperator
from include.config import conn_ids, owners, s3_buckets, stages
from include.config.email_lists import data_integration_support
from task_configs.dag_config.builder_config import hdyh_column_list as column_list

from airflow import DAG

default_args = {
    'start_date': pendulum.datetime(2025, 1, 7, tz='America/Los_Angeles'),
    'retries': 0,
    'owner': owners.data_integrations,
    'email': data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id='edm_inbound_builder_hdyh',
    default_args=default_args,
    schedule='30 10 * * *',
    catchup=False,
)

s3_prefix = "lake/lake.builder.hdyh/v1/fabletics"
request_params = {"includeUnpublished": "true"}
date_param = "{{macros.datetime.now().strftime('%Y%m%d')}}"

with dag:

    builder_to_s3 = BuilderHDYHToS3Operator(
        task_id='builder_fl_hdyh_to_s3',
        path='hdyh',
        builder_conn_id='builder_fabletics',
        bucket=s3_buckets.tsos_da_int_inbound,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        column_list=[x.source_name for x in column_list],
        key=f"{s3_prefix}/builder_hdyh_{date_param}.gz",
        request_params=request_params,
    )

    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="load_to_snowflake",
        database="lake",
        staging_database='lake_stg',
        schema='builder',
        table='hdyh',
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}",
        copy_config=CopyConfigCsv(field_delimiter='\t', header_rows=0, skip_pct=1),
    )

    hdyh_flattened = SnowflakeProcedureOperator(
        procedure='builder.hdyh_flattened.sql',
        database='lake',
        watermark_tables=["lake.builder.hdyh"],
    )

    builder_to_s3 >> to_snowflake >> hdyh_flattened
