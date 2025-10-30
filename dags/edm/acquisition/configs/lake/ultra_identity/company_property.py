from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultraidentity',
    schema='dbo',
    table='company_property',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('company_property_id', 'INT', uniqueness=True),
        Column('company_id', 'INT'),
        Column('key', 'VARCHAR(255)', source_name="[key]"),
        Column('value', 'VARCHAR(8000)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
