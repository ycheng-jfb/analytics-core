import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.excel_smb import ExcelSMBToS3Operator, SheetConfig
from include.airflow.operators.snowflake_load import SnowflakeInsertOperator
from include.config import conn_ids, email_lists, owners, s3_buckets, stages
from include.utils.snowflake import Column, CopyConfigCsv

sheets = [
    SheetConfig(
        sheet_name='Ingestion Sheet',
        schema='excel',
        table='jfb_amazon_assortment',
        header_rows=1,
        column_list=[
            Column('po_number', 'STRING', uniqueness=True, source_name='PO_Number'),
            Column('sku', 'STRING', source_name='SKU'),
            Column('launch_month', 'DATETIME', source_name='Launch_Month'),
            Column('eta', 'DATETIME', source_name='ETA'),
            Column('amazon_on_order', 'NUMBER(38,0)', source_name='Amazon_On_Order'),
        ],
        default_schema_version='v2',
    ),
]


default_args = {
    "start_date": pendulum.datetime(2023, 1, 1, 0, tz="America/Los_Angeles"),
    "retries": 3,
    "owner": owners.jfb_analytics,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id='edm_inbound_jfb_amazon_assortment',
    default_args=default_args,
    schedule='0 10 * * 2',
    catchup=False,
    max_active_runs=1,
)
with dag:
    to_s3 = ExcelSMBToS3Operator(
        task_id='excel_to_s3',
        smb_path='/Inbound/airflow.amazon_assortment/Amazon 2023 Assortment.xlsx',
        share_name='BI',
        bucket=s3_buckets.tsos_da_int_inbound,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        smb_conn_id=conn_ids.SMB.nas01,
        sheet_configs=sheets,
        default_schema_version='v2',
        is_archive_file=True,
        archive_folder='archive',
    )
    for sheet in sheets:
        to_snowflake = SnowflakeInsertOperator(
            task_id=f"{sheet.schema}.{sheet.table}.load_to_snowflake",
            files_path=f"{stages.tsos_da_int_inbound}/lake/{sheet.schema}.{sheet.table}/v2/",
            database='lake',
            staging_database='lake_stg',
            view_database='lake_view',
            schema='excel',
            table=sheet.table,
            column_list=sheet.column_list,
            initial_load=True,
            copy_config=CopyConfigCsv(
                field_delimiter='|',
                record_delimiter='\n',
                header_rows=sheet.header_rows,
                skip_pct=1,
            ),
        )
        to_s3 >> to_snowflake
