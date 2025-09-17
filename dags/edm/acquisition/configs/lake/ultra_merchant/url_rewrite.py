from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='url_rewrite',
    schema_version_prefix='v2',
    column_list=[
        Column('url_rewrite_id', 'INT', uniqueness=True),
        Column('store_group_id', 'INT'),
        Column('path', 'VARCHAR(255)'),
        Column('fuseaction', 'VARCHAR(50)'),
        Column('page_key', 'VARCHAR(50)'),
        Column('target', 'VARCHAR(255)'),
        Column('attribute_list', 'VARCHAR(255)'),
        Column('title', 'VARCHAR(100)'),
        Column('keywords', 'VARCHAR(500)'),
        Column('description', 'VARCHAR(200)'),
        Column('notes', 'VARCHAR(500)'),
        Column('active', 'INT'),
        Column('is_global', 'INT'),
    ],
)
