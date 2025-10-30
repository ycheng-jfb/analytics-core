from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='gdpr',
    schema='dbo',
    table='statuscode',
    watermark_column='statuscode',
    initial_load_value='0',
    schema_version_prefix='v2',
    strict_inequality=True,
    column_list=[
        Column('statuscode', 'INT', uniqueness=True, delta_column=True),
        Column('label', 'VARCHAR(50)'),
    ],
)
