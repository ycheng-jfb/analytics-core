from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='stage_tracking',
    watermark_column='datetime_modified',
    initial_load_value='2020-05-11 05:00:00',
    schema_version_prefix='v2',
    column_list=[
        Column('stage_tracking_id', 'INT', uniqueness=True),
        Column('session_id', 'INT'),
        Column('offer_id', 'INT'),
        Column('last_stage_code', 'VARCHAR(25)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('statuscode', 'INT'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
