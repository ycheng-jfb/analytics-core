from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='test_metadata',
    watermark_column='datetime_modified',
    strict_inequality=True,
    schema_version_prefix='v2',
    column_list=[
        Column('test_metadata_id', 'INT', uniqueness=True),
        Column('name', 'VARCHAR(255)'),
        Column('label', 'VARCHAR(255)'),
        Column('store_group_id', 'INT'),
        Column('control_pct', 'INT'),
        Column('variant_count', 'INT'),
        Column('statuscode', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('datetime_activated', 'TIMESTAMP_NTZ(3)'),
    ],
)
