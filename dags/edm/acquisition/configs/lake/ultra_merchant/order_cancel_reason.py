from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='order_cancel_reason',
    schema_version_prefix='v2',
    column_list=[
        Column('order_cancel_reason_id', 'INT', uniqueness=True),
        Column('store_group_id', 'INT'),
        Column('label', 'VARCHAR(255)'),
        Column('access', 'VARCHAR(15)'),
        Column('sort', 'INT'),
    ],
)
