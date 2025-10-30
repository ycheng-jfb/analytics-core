from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='customer_fitmatch_product',
    watermark_column='datetime_modified',
    column_list=[
        Column('customer_fitmatch_product_id', 'INT', uniqueness=True),
        Column('customer_fitmatch_id', 'INT'),
        Column('product_id', 'INT'),
        Column('sku', 'VARCHAR(50)'),
        Column('size', 'VARCHAR(50)'),
        Column('style', 'VARCHAR(50)'),
        Column('score', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
