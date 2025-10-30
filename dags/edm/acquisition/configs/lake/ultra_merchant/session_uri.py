from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='session_uri',
    watermark_column='datetime_modified',
    enable_archive=False,
    partition_cols=['session_uri_id', 'session_uri_hash_id'],
    schema_version_prefix='v2',
    column_list=[
        Column('session_uri_id', 'INT', uniqueness=True),
        Column('session_uri_hash_id', 'INT'),
        Column('session_id', 'INT'),
        Column('referer', 'VARCHAR'),
        Column('uri', 'VARCHAR(4096)'),
        Column('user_agent', 'VARCHAR(512)'),
        Column('device_type_id', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
