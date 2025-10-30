from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='product_wait_list_type',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('product_wait_list_type_id', 'INT', uniqueness=True),
        Column('store_group_id', 'INT'),
        Column('label', 'VARCHAR(100)'),
        Column('code', 'VARCHAR(25)'),
        Column('is_active', 'BOOLEAN'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
