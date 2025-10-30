from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='return_disposition',
    schema_version_prefix='v2',
    column_list=[
        Column('return_disposition_id', 'INT', uniqueness=True),
        Column('label', 'VARCHAR(50)'),
        Column('description', 'VARCHAR(255)'),
    ],
)
