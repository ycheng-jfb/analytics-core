from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='return_line',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('return_line_id', 'INT', uniqueness=True),
        Column('return_id', 'INT'),
        Column('order_line_id', 'INT'),
        Column('product_id', 'INT'),
        Column('condition', 'VARCHAR(25)'),
        Column('quantity', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
