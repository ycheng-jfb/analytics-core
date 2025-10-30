from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='featured_product',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('featured_product_id', 'INT', uniqueness=True),
        Column('featured_product_location_id', 'INT'),
        Column('object', 'VARCHAR(50)'),
        Column('object_id', 'INT'),
        Column('product_id', 'INT'),
        Column('sort', 'INT'),
        Column('active', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_start', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_end', 'TIMESTAMP_NTZ(3)'),
        Column('sale_price', 'NUMBER(19, 4)'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
