from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultrawarehouse',
    schema='dbo',
    table='device_log',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('device_log_id', 'INT', uniqueness=True),
        Column('device_id', 'INT'),
        Column('warehouse_id', 'INT'),
        Column('administrator_id', 'INT'),
        Column('datetime_checked_out', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_assigned', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_checked_in', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('status_code_id', 'INT'),
        Column('datetime_missing', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_needs_tech', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_removed', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_with_it', 'TIMESTAMP_NTZ(3)'),
    ],
)
