from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultracart',
    schema='dbo',
    table='cart',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('cart_id', 'INT', uniqueness=True),
        Column('cart_hash_id', 'INT', uniqueness=True),
        Column('cart_source_type_id', 'INT'),
        Column('store_group_id', 'INT'),
        Column('session_id', 'INT'),
        Column('customer_id', 'INT'),
        Column('code', 'VARCHAR(36)'),
        Column('cart_object', 'VARCHAR'),
        Column('statuscode', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
