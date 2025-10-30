from datetime import timedelta

import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.alchemer import AlchemerGetSurveyResponses
from include.airflow.operators.snowflake_load import SnowflakeIncrementalLoadOperator
from include.config import conn_ids, email_lists, owners, s3_buckets, snowflake_roles, stages
from include.utils.snowflake import CopyConfigCsv
from task_configs.dag_config.alchemer_config import column_list

default_args = {
    "start_date": pendulum.datetime(2020, 4, 1, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
    "email": email_lists.data_integration_support,
    'on_failure_callback': slack_failure_edm,
}

dag = DAG(
    dag_id="analytics_inbound_alchemer_survey_responses",
    default_args=default_args,
    schedule="45 1 * * *",
    catchup=False,
    max_active_runs=1,
)

database = 'lake'
schema = 'alchemer'
table = 'survey_responses'
yr_mth = "{{macros.datetime.now().strftime('%Y%m')}}"
s3_key = f'lake/{database}.{schema}.{table}/v4'

with dag:
    to_s3 = AlchemerGetSurveyResponses(
        task_id='to_s3',
        bucket=s3_buckets.tsos_da_int_inbound,
        key=f'{s3_key}/{yr_mth}/{schema}_{table}_{{{{ ts_nodash }}}}.csv.gz',
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        column_list=[col.source_name for col in column_list],
        write_header=True,
        process_name='alchemer_survey_responses',
        namespace='alchemer',
    )

    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id='to_snowflake',
        database=database,
        schema=schema,
        table=table,
        staging_database='lake_stg',
        view_database='lake_view',
        snowflake_conn_id=conn_ids.Snowflake.default,
        role=snowflake_roles.etl_service_account,
        column_list=column_list,
        files_path=f'{stages.tsos_da_int_inbound}/{s3_key}',
        copy_config=CopyConfigCsv(field_delimiter='\t', header_rows=1),
    )

    to_s3 >> to_snowflake
