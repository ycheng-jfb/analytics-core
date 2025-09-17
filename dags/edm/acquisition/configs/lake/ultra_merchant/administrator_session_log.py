from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='administrator_session_log',
    watermark_column='datetime_added',
    schema_version_prefix='v2',
    column_list=[
        Column('administrator_session_log_id', 'INT', uniqueness=True),
        Column('administrator_session_id', 'INT'),
        Column('object', 'VARCHAR(50)'),
        Column('object_id', 'INT'),
        Column('comment', 'VARCHAR(255)'),
        Column('query_string', 'VARCHAR(255)'),
        Column('ip', 'VARCHAR(15)'),
        Column('data_1', 'VARCHAR(255)'),
        Column('data_2', 'VARCHAR(255)'),
        Column('data_3', 'VARCHAR(255)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
