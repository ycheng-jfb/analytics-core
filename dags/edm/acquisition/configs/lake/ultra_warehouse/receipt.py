from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultrawarehouse',
    schema='dbo',
    table='receipt',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('receipt_id', 'INT', uniqueness=True),
        Column('type_code_id', 'INT'),
        Column('warehouse_id', 'INT'),
        Column('label', 'VARCHAR(255)'),
        Column('po_number', 'VARCHAR(255)'),
        Column('administrator_id', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('erp_status_code_id', 'INT'),
        Column('status_code_id', 'INT'),
        Column('company_id', 'INT'),
        Column('in_transit_id', 'INT'),
        Column('datetime_received', 'TIMESTAMP_NTZ(3)'),
        Column('in_transit_document_id', 'INT'),
        Column('datetime_transaction', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_local_transaction', 'TIMESTAMP_NTZ(3)'),
    ],
)
