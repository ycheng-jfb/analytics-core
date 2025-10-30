from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultrawarehouse',
    schema='dbo',
    table='retail_location_configuration',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('retail_location_configuration_id', 'INT', uniqueness=True),
        Column('retail_configuration_id', 'INT'),
        Column('retail_location_id', 'INT'),
        Column('configuration_value', 'VARCHAR(255)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
