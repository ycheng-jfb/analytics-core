import pendulum
from include.config import owners
from include.config.email_lists import data_integration_support
from include.config.stages import tsos_da_int_inbound
from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.dag_helpers import TFGControlOperator
from include.airflow.operators.builder import BuilderABTestingToS3Operator
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.snowflake_load import SnowflakeIncrementalLoadOperator
from include.config import conn_ids, s3_buckets
from include.utils.snowflake import CopyConfigCsv
from task_configs.dag_config.builder_config import config_list, db_configs

from airflow import DAG

default_args = {
    'start_date': pendulum.datetime(2021, 10, 7, tz='America/Los_Angeles'),
    'retries': 0,
    'owner': owners.data_integrations,
    'email': data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id='edm_inbound_builder_ab_testing_report',
    default_args=default_args,
    schedule='30 2,10,18 * * *',
    catchup=False,
)

with dag:
    date_param = "{{macros.datetime.now().strftime('%Y%m%d')}}"

    tfg_control = TFGControlOperator()

    builder_api_metadata_unmatched_variations = SnowflakeProcedureOperator(
        procedure='builder.builder_api_metadata_unmatched_variations.sql',
        database='lake',
    )

    for db_config in db_configs.values():
        to_snowflake = SnowflakeIncrementalLoadOperator(
            task_id=f'{db_config.name}_data_to_snowflake',
            database=db_config.db,
            schema=db_config.schema,
            table=db_config.get_table,
            staging_database='lake_stg',
            column_list=db_config.column_list,
            files_path=f'{tsos_da_int_inbound}/{db_config.get_s3_path}{date_param}',
            copy_config=CopyConfigCsv(field_delimiter='\t'),
            initial_load=True,
        )
        tfg_control >> to_snowflake >> builder_api_metadata_unmatched_variations

    for conf in config_list:
        builder_to_s3 = BuilderABTestingToS3Operator(
            task_id=f'{conf.tfg_brand}_{conf.content_type}_data_from_builder_to_s3',
            path=conf.content_type,
            builder_conn_id=conf.public_key_conn_id,
            request_params={
                'limit': 100,
                'offset': 0,
            },
            bucket=s3_buckets.tsos_da_int_inbound,
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            all_components_key=f"{db_configs['builder_ab_testing_report'].get_s3_path}{date_param}"
            f"/{conf.content_type}/{conf.tfg_brand}/{{{{ ts_nodash }}}}.csv.gz",
            all_targeting_components_key=f"{db_configs['builder_api_targeting_report'].get_s3_path}"
            f"{date_param}/{conf.content_type}/{conf.tfg_brand}/{{{{ ts_nodash }}}}.csv.gz",
            all_called_tests_column_key=f"{db_configs['called_tests_report'].get_s3_path}"
            f"{date_param}/{conf.content_type}/{conf.tfg_brand}/{{{{ ts_nodash }}}}.csv.gz",
            all_components_column_list=[
                col.name for col in db_configs['builder_ab_testing_report'].column_list
            ],
            all_targeting_components_column_list=[
                col.name for col in db_configs['builder_api_targeting_report'].column_list
            ],
            all_called_tests_column_list=[
                col.name for col in db_configs['called_tests_report'].column_list
            ],
            brand=conf.tfg_brand,
        )

        builder_to_s3 >> tfg_control
