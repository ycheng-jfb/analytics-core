from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='quiz',
    watermark_column='datetime_added',
    schema_version_prefix='v2',
    column_list=[
        Column('quiz_id', 'INT', uniqueness=True),
        Column('store_group_id', 'INT'),
        Column('quiz_type_id', 'INT'),
        Column('root_quiz_id', 'INT'),
        Column('label', 'VARCHAR(50)'),
        Column('page_count', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('statuscode', 'INT'),
    ],
)
