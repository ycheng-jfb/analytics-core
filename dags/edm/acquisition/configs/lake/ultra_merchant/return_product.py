from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='return_product',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('return_product_id', 'INT', uniqueness=True),
        Column('return_id', 'INT'),
        Column('order_line_id', 'INT'),
        Column('rma_product_id', 'INT'),
        Column('product_id', 'INT'),
        Column('item_id', 'INT'),
        Column('expected_return_condition_id', 'INT'),
        Column('return_condition_id', 'INT'),
        Column('return_disposition_id', 'INT'),
        Column('lpn_code', 'VARCHAR(25)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
