from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='session',
    watermark_column='datetime_modified',
    initial_load_value='2020-01-15',
    partition_cols=['session_id', 'session_hash_id'],
    schema_version_prefix='v2',
    column_list=[
        Column('session_id', 'INT', uniqueness=True),
        Column('session_hash_id', 'INT'),
        Column('store_id', 'INT'),
        Column('store_domain_id', 'INT'),
        Column('customer_id', 'INT'),
        Column('dm_gateway_id', 'INT'),
        Column('dm_site_id', 'INT'),
        Column('dm_gateway_test_site_id', 'INT'),
        Column('order_tracking_id', 'INT'),
        Column('session_key', 'VARCHAR(36)'),
        Column('ip', 'VARCHAR(15)'),
        Column('statuscode', 'INT'),
        Column('original_store_domain_id', 'INT'),
        Column('membership_level_id', 'INT'),
        Column('date_added', 'DATE'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
