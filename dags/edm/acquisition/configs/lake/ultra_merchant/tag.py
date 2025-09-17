from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='tag',
    watermark_column='datetime_modified',
    strict_inequality=True,
    schema_version_prefix='v2',
    column_list=[
        Column('tag_id', 'INT', uniqueness=True),
        Column('store_group_id', 'INT'),
        Column('tag_category_id', 'INT'),
        Column('parent_tag_id', 'INT'),
        Column('brand_id', 'INT'),
        Column('label', 'VARCHAR(100)'),
        Column('active', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('hide', 'BOOLEAN'),
    ],
)
