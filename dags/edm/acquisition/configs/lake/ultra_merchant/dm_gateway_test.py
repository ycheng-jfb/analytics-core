from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='dm_gateway_test',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('dm_gateway_test_id', 'INT', uniqueness=True),
        Column('dm_gateway_id', 'INT'),
        Column('label', 'VARCHAR(50)'),
        Column('description', 'VARCHAR'),
        Column('hypothesis', 'VARCHAR'),
        Column('results', 'VARCHAR'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_start', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_end', 'TIMESTAMP_NTZ(3)'),
        Column('statuscode', 'INT'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
