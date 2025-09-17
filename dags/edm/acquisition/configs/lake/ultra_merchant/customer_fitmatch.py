from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='customer_fitmatch',
    schema_version_prefix='v2',
    watermark_column='datetime_modified',
    column_list=[
        Column('customer_fitmatch_id', 'INT', uniqueness=True),
        Column('customer_id', 'INT'),
        Column('store_id', 'INT'),
        Column('store_group_id', 'INT'),
        Column('scan_id', 'VARCHAR(255)'),
        Column('fitmatch_user_id', 'VARCHAR(255)'),
        Column('bust_size', 'VARCHAR(50)'),
        Column('band_size', 'VARCHAR(50)'),
        Column('active', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('datetime_registered', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_scan_started', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_scan_completed', 'TIMESTAMP_NTZ(3)'),
    ],
)
