import pendulum
from airflow import DAG

import task_configs.dag_config.centric_config as centric_config
from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.dag_helpers import TFGControlOperator
from include.airflow.operators.postgres import PostgresToS3Operator, PostgresToS3WatermarkOperator
from include.airflow.operators.snowflake_load import (
    SnowflakeIncrementalLoadOperator,
    SnowflakeTruncateAndLoadOperator,
)
from include.config import conn_ids, email_lists, owners, s3_buckets, snowflake_roles, stages
from include.utils.snowflake import CopyConfigCsv

default_args = {
    "start_date": pendulum.datetime(2022, 2, 9, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    'on_failure_callback': slack_failure_edm,
}

dag = DAG(
    dag_id="edm_inbound_centric",
    default_args=default_args,
    schedule="45 2 * * *",
    catchup=False,
    max_active_tasks=5,
)

yr_mth = "{{macros.datetime.now().strftime('%Y%m')}}"

with dag:
    tfg_control = TFGControlOperator()
    for cfg in centric_config.watermark_table_list:
        s3_prefix = f'lake/{cfg.database}.{cfg.schema}.{cfg.table}/{cfg.version}'
        to_s3 = PostgresToS3WatermarkOperator(
            task_id=f'to_s3_{cfg.table}',
            db_conn_id=cfg.db_conn_id,
            bucket=s3_buckets.tsos_da_int_inbound,
            key=f'{s3_prefix}/{yr_mth}/{cfg.schema}_{cfg.table}_{{{{ ts_nodash }}}}.csv.gz',
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            column_list=cfg.column_list,
            watermark_column=cfg.watermark_column,
            namespace='centric_pg_inbound',
            process_name=f'{cfg.database}.{cfg.schema}.{cfg.table}',
            table=cfg.table,
            schema='public',
            database='exportdb',
        )

        to_snowflake = SnowflakeIncrementalLoadOperator(
            task_id=f'to_snowflake_{cfg.table}',
            database=cfg.database,
            schema=cfg.schema,
            table=cfg.table,
            staging_database='lake_stg',
            view_database='lake_view',
            snowflake_conn_id=conn_ids.Snowflake.default,
            role=snowflake_roles.etl_service_account,
            column_list=cfg.column_list,
            files_path=f'{stages.tsos_da_int_inbound}/{s3_prefix}',
            copy_config=CopyConfigCsv(field_delimiter='\t', header_rows=1),
        )

        tfg_control >> to_s3 >> to_snowflake

    for cfg in centric_config.no_watermark_table_list:
        s3_prefix = f'lake/{cfg.database}.{cfg.schema}.{cfg.table}/{cfg.version}'
        to_s3 = PostgresToS3Operator(
            task_id=f'to_s3_{cfg.table}',
            db_conn_id=cfg.db_conn_id,
            bucket=s3_buckets.tsos_da_int_inbound,
            key=f'{s3_prefix}/{yr_mth}/{cfg.schema}_{cfg.table}_{{{{ ts_nodash }}}}.csv.gz',
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            column_list=cfg.column_list,
            table=cfg.table,
            schema='public',
            database='exportdb',
        )

        to_snowflake = SnowflakeIncrementalLoadOperator(
            task_id=f'to_snowflake_{cfg.table}',
            database=cfg.database,
            schema=cfg.schema,
            table=cfg.table,
            staging_database='lake_stg',
            view_database='lake_view',
            snowflake_conn_id=conn_ids.Snowflake.default,
            role=snowflake_roles.etl_service_account,
            column_list=cfg.column_list,
            files_path=f'{stages.tsos_da_int_inbound}/{s3_prefix}',
            copy_config=CopyConfigCsv(field_delimiter='\t', header_rows=1),
        )

        tfg_control >> to_s3 >> to_snowflake

    for cfg in centric_config.truncate_load_table_list:
        s3_prefix = f'lake/{cfg.database}.{cfg.schema}.{cfg.table}/{cfg.version}'
        to_s3 = PostgresToS3Operator(
            task_id=f'to_s3_{cfg.table}',
            db_conn_id=cfg.db_conn_id,
            bucket=s3_buckets.tsos_da_int_inbound,
            key=f'{s3_prefix}/{yr_mth}/{cfg.schema}_{cfg.table}_{{{{ ts_nodash }}}}.csv.gz',
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            column_list=cfg.column_list,
            table=cfg.table,
            schema='public',
            database='exportdb',
        )

        to_snowflake = SnowflakeTruncateAndLoadOperator(
            task_id=f'to_snowflake_{cfg.table}',
            database=cfg.database,
            schema=cfg.schema,
            table=cfg.table,
            staging_database='lake_stg',
            view_database='lake_view',
            snowflake_conn_id=conn_ids.Snowflake.default,
            role=snowflake_roles.etl_service_account,
            column_list=cfg.column_list,
            files_path=f'{stages.tsos_da_int_inbound}/{s3_prefix}',
            copy_config=CopyConfigCsv(field_delimiter='\t', header_rows=1),
        )

        tfg_control >> to_s3 >> to_snowflake
