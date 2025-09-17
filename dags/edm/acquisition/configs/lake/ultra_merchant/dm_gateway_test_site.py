from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='dm_gateway_test_site',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('dm_gateway_test_site_id', 'INT', uniqueness=True),
        Column('dm_gateway_test_id', 'INT'),
        Column('dm_site_id', 'INT'),
        Column('dm_gateway_test_site_type_id', 'INT'),
        Column('dm_gateway_test_site_class_id', 'INT'),
        Column('dm_gateway_test_site_location_id', 'INT'),
        Column('weight', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
