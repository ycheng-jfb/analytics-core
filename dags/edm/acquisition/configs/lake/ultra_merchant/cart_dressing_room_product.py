from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='cart_dressing_room_product',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('cart_dressing_room_product_id', 'INT', uniqueness=True),
        Column('cart_dressing_room_id', 'INT'),
        Column('product_id', 'INT'),
        Column('lpn_code', 'VARCHAR(35)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
