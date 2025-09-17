from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='return_batch',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('return_batch_id', 'INT', uniqueness=True),
        Column('label', 'VARCHAR(255)'),
        Column('warehouse_id', 'INT'),
        Column('store_id', 'INT'),
        Column('administrator_id_created', 'INT'),
        Column('administrator_id_approved', 'INT'),
        Column('statuscode', 'INT'),
        Column('date_added', 'TIMESTAMP_NTZ(3)'),
        Column('date_approved', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
