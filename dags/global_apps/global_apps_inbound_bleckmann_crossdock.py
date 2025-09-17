import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.excel_smb import ExcelSMBToS3BatchOperator, SheetConfig
from include.airflow.operators.snowflake_load import SnowflakeIncrementalLoadOperator
from include.config import conn_ids, email_lists, owners, s3_buckets, stages
from include.utils.snowflake import Column, CopyConfigCsv

sheets = [
    SheetConfig(
        sheet_name='Sheet1',
        schema='excel',
        table='bleckmann_crossdock',
        header_rows=1,
        column_list=[
            Column('load_number', 'NUMBER(38,0)', uniqueness=True),
            Column('brand', 'STRING'),
            Column('type', 'STRING'),
            Column('date_collection', 'DATE'),
            Column('date_delivery', 'DATE', delta_column=True),
            Column('lovos', 'STRING'),
            Column('plates', 'STRING'),
            Column('rate', 'NUMBER(38,4)'),
            Column('fuel_charge', 'NUMBER(38,4)'),
            Column('total', 'NUMBER(38,4)'),
            Column('business_unit', 'STRING'),
            Column('cargo_per', 'NUMBER(20,4)'),
            Column('invoice_number', 'STRING', uniqueness=True),
            Column('invoice_date', 'DATE', delta_column=True),
        ],
    ),
]
uk_sheets = [
    SheetConfig(
        sheet_name='Sheet1',
        schema='excel',
        table='bleckmann_uk',
        header_rows=1,
        column_list=[
            Column(name='entry_status', type='STRING', source_name='Entry Status'),
            Column(name='last_tx_status', type='STRING', source_name='Last Tx Status'),
            Column(name='hmrc_type', type='STRING', source_name='HMRC Type'),
            Column(name='declaration_type', type='STRING', source_name='Declaration Type'),
            Column(name='consignee_reference', type='STRING', source_name='Consignee Reference'),
            Column(name='inventory_reference', type='STRING', source_name='Inventory Reference'),
            Column(name='created', type='STRING', source_name='Created'),
            Column(name='reference', type='STRING', source_name='Reference'),
            Column(name='consignor', type='STRING', source_name='Consignor'),
            Column(name='consignee', type='STRING', source_name='Consignee'),
            Column(name='customer', type='STRING', source_name='Customer'),
            Column(name='number_of_packages', type='NUMBER(38, 0)', source_name='No Packages'),
            Column(name='gross_weight', type='NUMBER(38, 10)', source_name='Gross Weight'),
            Column(name='volume', type='NUMBER(38, 10)', source_name='Volume'),
            Column(name='transport_id', type='STRING', source_name='Transport ID'),
            Column(name='business_unit', type='STRING', source_name='Business Unit'),
            Column(name='invoice_number', type='NUMBER(38, 0)', source_name='Invoice Number'),
            Column(name='invoice_date', type='DATE', source_name='Invoice Date'),
            Column(name='order_id', type='NUMBER(38, 0)', source_name='Order ID'),
            Column(name='vessel', type='STRING', source_name='Vessel'),
            Column(name='report_date', type='DATE', source_name='Report Date'),
            Column(name='entry_date', type='DATE', source_name='Entry Date'),
            Column(name='entry_number', type='STRING', source_name='Entry Number'),
            Column(name='chief_route', type='STRING', source_name='CHIEF Route'),
            Column(
                name='currency_invoice_total',
                type='NUMBER(38, 10)',
                source_name='Currency Invoice Total',
            ),
            Column(
                name='cds_invoice_total', type='NUMBER(38, 10)', source_name='CDS Invoice Total'
            ),
            Column(name='customs_duty_gbp', type='NUMBER(38, 10)', source_name='Customs Duty GBP'),
            Column(
                name='clearance_cost_eur', type='NUMBER(38, 10)', source_name='Clearance Cost EUR'
            ),
            Column(
                name='exchange_rate_gbp_euro',
                type='NUMBER(38, 10)',
                source_name='EXCHANGE RATE GBP-EURO',
            ),
            Column(
                name='duties_taxes_euro', type='NUMBER(38, 10)', source_name='Duties & Taxes EURO'
            ),
            Column(name='prepayment_euro', type='NUMBER(38, 10)', source_name='Prepayment EURO'),
            Column(
                name='total_amount_euro', type='NUMBER(38, 10)', source_name='Total Amount EURO'
            ),
            Column(name='customs_vat', type='NUMBER(38, 10)', source_name='Customs VAT'),
            Column(
                name='customs_other_duty', type='NUMBER(38, 10)', source_name='Customs Other Duty'
            ),
            Column(name='deferment_1', type='STRING', source_name='Deferment Number1'),
            Column(name='deferment_2', type='STRING', source_name='Deferment Number2'),
        ],
    ),
]

combined_sheets = sheets + uk_sheets

default_args = {
    'start_date': pendulum.datetime(2020, 3, 1, tz='America/Los_Angeles'),
    'retries': 1,
    'owner': owners.data_integrations,
    'email': email_lists.data_integration_support,
    'on_failure_callback': slack_failure_gsc,
}

dag = DAG(
    dag_id='global_apps_inbound_bleckmann_crossdock',
    default_args=default_args,
    schedule='0 9 * * *',
    catchup=False,
    max_active_tasks=100,
    max_active_runs=1,
)
with dag:
    to_s3 = ExcelSMBToS3BatchOperator(
        task_id='excel_to_s3',
        smb_dir='Inbound/airflow.bleckmann_crossdock',
        share_name='BI',
        file_pattern_list=['BLECKMANN_CROSSDOCK.xlsx'],
        bucket=s3_buckets.tsos_da_int_inbound,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        smb_conn_id=conn_ids.SMB.nas01,
        is_archive_file=True,
        sheet_configs=sheets,
        remove_header_new_lines=True,
        skip_downstream_if_no_files=True,
    )

    uk_sheets_to_s3 = ExcelSMBToS3BatchOperator(
        task_id='uk_excel_to_s3',
        smb_dir='Inbound/airflow.bleckmann_crossdock',
        share_name='BI',
        file_pattern_list=['Techstyle_UK_import_*.xlsx'],
        bucket=s3_buckets.tsos_da_int_inbound,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        smb_conn_id=conn_ids.SMB.nas01,
        is_archive_file=True,
        sheet_configs=uk_sheets,
        remove_header_new_lines=True,
        skip_downstream_if_no_files=True,
    )

    for sheet in sheets:
        bleckmansheet_load_to_snowflake = SnowflakeIncrementalLoadOperator(
            task_id=f"load_to_snowflake_{sheet.table}",
            files_path=f"{stages.tsos_da_int_inbound}/lake/{sheet.schema}.{sheet.table}/"
            f"{sheet.default_schema_version}/",
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

        to_s3 >> bleckmansheet_load_to_snowflake

    for uk_sheet in uk_sheets:
        uk_sheet_load_to_snowflake = SnowflakeIncrementalLoadOperator(
            task_id=f"load_to_snowflake_{uk_sheet.table}",
            files_path=f"{stages.tsos_da_int_inbound}/lake/{uk_sheet.schema}.{uk_sheet.table}/"
            f"{uk_sheet.default_schema_version}/",
            database='lake',
            staging_database='lake_stg',
            view_database='lake_view',
            schema='excel',
            table=uk_sheet.table,
            column_list=uk_sheet.column_list,
            initial_load=True,
            copy_config=CopyConfigCsv(
                field_delimiter='|',
                record_delimiter='\n',
                header_rows=uk_sheet.header_rows,
                skip_pct=1,
            ),
        )

        uk_sheets_to_s3 >> uk_sheet_load_to_snowflake
