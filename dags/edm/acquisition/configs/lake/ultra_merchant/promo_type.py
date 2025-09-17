from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='promo_type',
    watermark_column='datetime_added',
    schema_version_prefix='v2',
    column_list=[
        Column('promo_type_id', 'INT', uniqueness=True),
        Column('label', 'VARCHAR(100)'),
        Column('processing_priority', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('show_cms', 'INT'),
        Column('cms_label', 'VARCHAR(50)'),
        Column('cms_description', 'VARCHAR(500)'),
    ],
)
