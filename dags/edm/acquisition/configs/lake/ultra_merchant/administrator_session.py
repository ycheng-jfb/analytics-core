from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='administrator_session',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('administrator_session_id', 'INT', uniqueness=True),
        Column('administrator_id', 'INT'),
        Column('session_key', 'VARCHAR(36)'),
        Column('ip', 'VARCHAR(15)'),
        Column('date_added', 'TIMESTAMP_NTZ(0)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('statuscode', 'INT'),
    ],
)
