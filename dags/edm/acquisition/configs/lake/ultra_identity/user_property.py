from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultraidentity',
    schema='dbo',
    table='user_property',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('user_property_id', 'INT', uniqueness=True),
        Column('user_id', 'INT'),
        Column('key', 'VARCHAR(255)', source_name="[key]"),
        Column('value', 'VARCHAR(8000)'),
        Column('application_id', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
