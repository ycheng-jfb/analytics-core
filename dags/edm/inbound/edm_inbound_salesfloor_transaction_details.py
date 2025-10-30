import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.sftp_to_s3 import SFTPToS3BatchOperator
from include.airflow.operators.snowflake_load import SnowflakeIncrementalLoadOperator
from include.config import conn_ids, email_lists, owners, s3_buckets, snowflake_roles
from include.utils.snowflake import CopyConfigCsv
from task_configs.dag_config.salesfloor_config import transaction_details_cfg as cfg

default_args = {
    "start_date": pendulum.datetime(2020, 4, 1, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    'on_failure_callback': slack_failure_edm,
}

dag = DAG(
    dag_id="edm_inbound_salesfloor_transaction_details",
    default_args=default_args,
    schedule="39 2 * * *",
    catchup=False,
)

yr_mth = "{{macros.datetime.now().strftime('%Y%m')}}"
with dag:
    to_s3 = SFTPToS3BatchOperator(
        task_id='to_s3',
        remote_dir='fabletics-prd/outbound/prd/transactions',
        file_pattern='FABLETICS_transactionsdetails_*',
        sftp_conn_id=conn_ids.SFTP.sftp_salesfloor,
        s3_bucket=s3_buckets.tsos_da_int_inbound,
        s3_prefix=f'{cfg.s3_prefix}/{yr_mth}',
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        remove_remote_files=True,
    )

    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id='to_snowflake',
        database=cfg.database,
        schema=cfg.schema,
        table=cfg.table,
        staging_database='lake_stg',
        view_database='lake_view',
        snowflake_conn_id=conn_ids.Snowflake.default,
        role=snowflake_roles.etl_service_account,
        column_list=cfg.column_list,
        files_path=None,
        copy_config=CopyConfigCsv(
            header_rows=1,
            null_if="'', 'N/A'",
        ),
        custom_select=cfg.custom_select,
    )

    to_s3 >> to_snowflake
