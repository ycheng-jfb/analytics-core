from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='zip_city_state',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('zip_city_state_id', 'INT', uniqueness=True),
        Column('zip', 'VARCHAR(5)'),
        Column('city', 'VARCHAR(35)'),
        Column('state', 'VARCHAR(2)'),
        Column('areacode', 'VARCHAR(3)'),
        Column('county', 'VARCHAR(50)'),
        Column('fips', 'VARCHAR(5)'),
        Column('timezone', 'VARCHAR(5)'),
        Column('dst_flag', 'VARCHAR(1)'),
        Column('latitude', 'DOUBLE'),
        Column('longitude', 'DOUBLE'),
        Column('type', 'VARCHAR(1)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
