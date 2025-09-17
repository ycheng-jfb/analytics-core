from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='product_product_category',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('product_category_id', 'INT', uniqueness=True),
        Column('product_id', 'INT', uniqueness=True),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
