from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultracms',
    schema='dbo',
    table='asset_image_type',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('asset_image_type_id', 'INT', uniqueness=True),
        Column('label', 'VARCHAR(25)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
