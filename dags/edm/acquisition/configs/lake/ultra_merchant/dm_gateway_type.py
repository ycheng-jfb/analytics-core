from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='dm_gateway_type',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('dm_gateway_type_id', 'INT', uniqueness=True),
        Column('label', 'VARCHAR(50)'),
        Column('sort', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('active', 'INT'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
