from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='membership_cancel_reason',
    schema_version_prefix='v2',
    column_list=[
        Column('membership_cancel_reason_id', 'INT', uniqueness=True),
        Column('store_group_id', 'INT'),
        Column('label', 'VARCHAR(100)'),
        Column('access', 'VARCHAR(15)'),
        Column('sort', 'INT'),
        Column('active', 'INT'),
        Column('suppress_email', 'INT'),
    ],
)
