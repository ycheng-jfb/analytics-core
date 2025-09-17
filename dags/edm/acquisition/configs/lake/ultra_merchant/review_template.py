from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='review_template',
    schema_version_prefix='v2',
    column_list=[
        Column('review_template_id', 'INT', uniqueness=True),
        Column('store_group_id', 'INT'),
        Column('label', 'VARCHAR(255)'),
        Column('description', 'VARCHAR(512)'),
        Column('pages', 'INT'),
    ],
)
