from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='stage_tracking_log',
    watermark_column='datetime_modified',
    initial_load_value='2020-05-11 10:00:00',
    schema_version_prefix='v2',
    column_list=[
        Column('stage_tracking_log_id', 'INT', uniqueness=True),
        Column('stage_tracking_id', 'INT'),
        Column('stage_code', 'VARCHAR(25)'),
        Column('object', 'VARCHAR(50)'),
        Column('object_id', 'INT'),
        Column('input_errors', 'INT'),
        Column('process_errors', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
