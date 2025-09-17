from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='meta_data',
    schema_version_prefix='v2',
    column_list=[
        Column('meta_data_id', 'INT', uniqueness=True),
        Column('object', 'VARCHAR(50)'),
        Column('object_id', 'INT'),
        Column('html_content', 'VARCHAR'),
        Column('field', 'VARCHAR(50)'),
        Column('field_content', 'VARCHAR'),
    ],
)
