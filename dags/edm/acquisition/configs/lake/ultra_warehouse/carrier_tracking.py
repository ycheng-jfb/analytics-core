from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultrawarehouse',
    schema='dbo',
    table='carrier_tracking',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('carrier_tracking_id', 'INT', uniqueness=True),
        Column('carrier_id', 'INT'),
        Column('object', 'VARCHAR(255)'),
        Column('object_id', 'VARCHAR(50)'),
        Column('tracking_number', 'VARCHAR(255)'),
        Column('event_code_id', 'VARCHAR(50)'),
        Column('release_exchange', 'BOOLEAN'),
        Column('is_delivered', 'BOOLEAN'),
        Column('datetime_event', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('comment', 'VARCHAR(255)'),
    ],
)
