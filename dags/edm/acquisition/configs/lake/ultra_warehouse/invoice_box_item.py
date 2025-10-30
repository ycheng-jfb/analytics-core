from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultrawarehouse',
    schema='dbo',
    table='invoice_box_item',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('invoice_box_item_id', 'INT', uniqueness=True),
        Column('invoice_box_id', 'INT'),
        Column('invoice_item_id', 'INT'),
        Column('quantity', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('lpn_code', 'VARCHAR(255)'),
        Column('lpn_id', 'INT'),
    ],
)
