from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='session_dm_gateway',
    watermark_column='datetime_modified',
    initial_load_value='2020-01-14',
    schema_version_prefix='v2',
    column_list=[
        Column('session_dm_gateway_id', 'INT', uniqueness=True),
        Column('session_id', 'INT'),
        Column('dm_gateway_id', 'INT'),
        Column('dm_site_id', 'INT'),
        Column('dm_gateway_test_site_id', 'INT'),
        Column('order_tracking_id', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('statuscode', 'INT'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
