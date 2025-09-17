import importlib

import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake_load import SnowflakeIncrementalLoadOperator
from include.config import conn_ids, email_lists, owners, s3_buckets, snowflake_roles, stages
from include.utils.snowflake import CopyConfigCsv
from task_configs.dag_config.sailpoint_config import config_list

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2022, 4, 1, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    'on_failure_callback': slack_failure_edm,
}

dag = DAG(
    dag_id="edm_operations_sailpoint",
    default_args=default_args,
    schedule="30 2 * * *",
    catchup=False,
)

with dag:
    for cfg in config_list:
        sailpoint_module = importlib.import_module('include.airflow.operators.sailpoint')
        to_s3_operator = getattr(sailpoint_module, cfg.sailpoint_operator)

        to_s3 = to_s3_operator(
            task_id=f'to_s3_{cfg.table}',
            hook_conn_id=conn_ids.Sailpoint.default,
            bucket=s3_buckets.tsos_da_int_inbound,
            key=f'{cfg.s3_prefix}/{cfg.yr_mth}/{cfg.schema}_{cfg.table}_{{{{ ts_nodash }}}}.csv.gz',
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            column_list=[x.source_name for x in cfg.column_list],
            write_header=True,
            source_ids=cfg.source_ids,
            url_endpoint=cfg.endpoint,
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
            files_path=f'{stages.tsos_da_int_inbound}/{cfg.s3_prefix}/',
            copy_config=CopyConfigCsv(field_delimiter='\t', header_rows=1),
        )

        to_s3 >> to_snowflake
