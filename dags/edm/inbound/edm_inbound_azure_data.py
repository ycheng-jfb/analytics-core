import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.azure_graph_to_s3 import AzureGraphToS3
from include.airflow.operators.snowflake_load import SnowflakeIncrementalLoadOperator
from include.config import conn_ids, owners, s3_buckets
from include.config.email_lists import data_integration_support
from include.config.stages import tsos_da_int_inbound
from include.utils.snowflake import CopyConfigCsv
from task_configs.dag_config.azure_config import config_list

default_args = {
    'start_date': pendulum.datetime(2021, 10, 7, tz='America/Los_Angeles'),
    'retries': 0,
    'owner': owners.data_integrations,
    'email': data_integration_support,
    "on_failure_callback": slack_failure_edm,
    "task_concurrency": 1,
}

dag = DAG(
    dag_id='edm_inbound_azure_data',
    default_args=default_args,
    schedule='30 2 * * *',
    catchup=False,
)

with dag:
    for conf in config_list:
        azure_to_s3 = AzureGraphToS3(
            task_id=f'{conf.name}_data_from_azure_to_s3',
            azure_url=conf.api_prefix + conf.get_url_column_list,
            key=conf.get_S3_path + conf.get_file_name,
            prefix_list=conf.prefix_list,
            group_request_url=conf.request_url,
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            column_list=[x.source_name for x in conf.column_list],
            bucket=s3_buckets.tsos_da_int_inbound,
            write_header=True,
        )

        to_snowflake = SnowflakeIncrementalLoadOperator(
            task_id=f'{conf.name}_data_to_snowflake',
            database=conf.db,
            schema=conf.schema,
            table=conf.get_table,
            staging_database='lake_stg',
            view_database='lake_view',
            snowflake_conn_id=conn_ids.Snowflake.default,
            column_list=conf.column_list,
            files_path=f'{tsos_da_int_inbound}/{conf.get_S3_path}',
            copy_config=CopyConfigCsv(field_delimiter='\t', header_rows=1),
        )

        azure_to_s3 >> to_snowflake
