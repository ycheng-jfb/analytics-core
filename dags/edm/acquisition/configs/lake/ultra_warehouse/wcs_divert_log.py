from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultrawarehouse',
    schema='dbo',
    table='wcs_divert_log',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('wcs_divert_log_id', 'INT', uniqueness=True),
        Column('object', 'VARCHAR(255)'),
        Column('object_id', 'INT'),
        Column('wcs_path_id', 'INT'),
        Column('wcs_divert_id', 'INT'),
        Column('wcs_lane_id', 'INT'),
        Column('actual_wcs_lane_id', 'INT'),
        Column('sorted_count', 'INT'),
        Column('datetime_notified', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_received', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_first_sorted', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_last_sorted', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_diverted', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_cancelled', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('status_code_id', 'INT'),
    ],
)
