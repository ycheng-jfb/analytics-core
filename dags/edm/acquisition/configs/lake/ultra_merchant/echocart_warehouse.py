from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='echocart_warehouse',
    watermark_column='datetime_modified',
    column_list=[
        Column('echocart_warehouse_id', 'INT', uniqueness=True),
        Column('echocart_id', 'INT'),
        Column('session_id', 'INT'),
        Column('customer_id', 'INT'),
        Column('warehouse_id', 'INT'),
        Column('item_count', 'INT'),
        Column('dressing_room_id', 'INT'),
        Column('retail_cart_type', 'VARCHAR(10)'),
        Column('administrator_id', 'INT'),
        Column('store_group_id', 'INT'),
        Column('statuscode', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('datetime_expired', 'TIMESTAMP_NTZ(3)'),
        Column('store_id', 'INT'),
    ],
)
