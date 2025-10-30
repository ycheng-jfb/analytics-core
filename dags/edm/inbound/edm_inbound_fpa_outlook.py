import pendulum
from airflow import DAG

from include.airflow.operators.smb_to_s3 import SMBToS3BatchOperator
from include.airflow.operators.snowflake_load import SnowflakeIncrementalLoadOperator
from include.config import conn_ids, s3_buckets, snowflake_roles, stages
from include.utils.snowflake import Column, CopyConfigCsv
from include.config import owners

outlook_sxf_column_list = [
    Column('DAY', 'VARCHAR(255)', uniqueness=True),
    Column('MONTH', 'VARCHAR(255)', uniqueness=True),
    Column('DATE_GRAIN', 'VARCHAR(255)'),
    Column('SOURCE', 'VARCHAR(255)'),
    Column('STORE', 'VARCHAR(255)', uniqueness=True),
    Column('NET_CASH_REVENUE', 'NUMERIC(19,4)'),
    Column('CASH_GROSS_MARGIN_AMOUNT', 'NUMERIC(19,4)'),
    Column('CASH_GROSS_MARGIN_PERCENT', 'NUMERIC(19,4)'),
    Column('ACTIVATING_SHIPPED_ORDER_COUNT', 'NUMERIC(19,4)'),
    Column('NONACTIVATING_SHIPPED_ORDER_COUNT', 'NUMERIC(19,4)'),
    Column('ACTIVATING_SHIPPED_ORDER_UNITS', 'NUMERIC(19,4)'),
    Column('NONACTIVATING_SHIPPED_ORDER_UNITS', 'NUMERIC(19,4)'),
    Column('ACTIVATING_SHIPPED_AOV_INCL_SHIPPING', 'NUMERIC(19,4)'),
    Column('NONACTIVATING_SHIPPED_AOV_INCL_SHIPPING', 'NUMERIC(19,4)'),
    Column('MEMBERSHIP_CREDIT_REDEEMED_AMOUNT', 'NUMERIC(19,4)'),
    Column('MEMBERSHIP_CREDIT_CHARGED_AMOUNT', 'NUMERIC(19,4)'),
    Column('MEMBERSHIP_CREDIT_CASH_REFUNDED_AMOUNT', 'NUMERIC(19,4)'),
    Column('NET_MEMBERSHIP_CREDIT_CHARGED_AMOUNT', 'NUMERIC(19,4)'),
    Column('MEDIA_SPEND', 'NUMERIC(19,4)'),
    Column('TOTAL_VIPS_ON_DATE', 'NUMERIC(19,4)'),
    Column('EOP_VIPS', 'NUMERIC(19,4)'),
]

outlook_sxf_s3_prefix = 'lake/finance_ingestions/lake.fpa.outlook_sxf/v2'

default_args = {
    "owner": owners.data_integrations,
}

dag = DAG(
    dag_id='edm_inbound_fpa_outlook',
    default_args=default_args,
    start_date=pendulum.datetime(2021, 4, 18, tz='America/Los_Angeles'),
    catchup=False,
    schedule='0 20 * * 1-2',
)

with dag:
    to_s3 = SMBToS3BatchOperator(
        compression=None,
        task_id='smb_to_s3',
        remote_dir='Inbound/airflow.finance_ingestions/lake.fpa.outlook_sxf/',
        s3_bucket=s3_buckets.tsos_da_int_inbound,
        s3_prefix=outlook_sxf_s3_prefix,
        share_name='BI',
        smb_conn_id=conn_ids.SMB.nas01,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        file_pattern_list=['SXF*.csv'],
        archive_remote_files=True,
        archive_folder='archive',
    )
    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id='fpa_outlook_sxf_to_snowflake',
        snowflake_conn_id=conn_ids.Snowflake.default,
        database='lake',
        schema='fpa',
        table='outlook_sxf',
        column_list=outlook_sxf_column_list,
        files_path=f'{stages.tsos_da_int_inbound}/{outlook_sxf_s3_prefix}',
        copy_config=CopyConfigCsv(header_rows=1),
        role=snowflake_roles.etl_service_account,
    )

to_s3 >> to_snowflake
