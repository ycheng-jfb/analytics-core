from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='membership_suggestion_product',
    watermark_column='datetime_modified',
    initial_load_value='2020-01-14',
    schema_version_prefix='v2',
    column_list=[
        Column('membership_suggestion_product_id', 'INT', uniqueness=True),
        Column('membership_suggestion_id', 'INT'),
        Column('product_id', 'INT'),
        Column('order_id', 'INT'),
        Column('clicked', 'INT'),
        Column('purchased', 'INT'),
        Column('viewed', 'INT'),
        Column('sort', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_viewed', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_purchased', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('membership_suggestion_product_type_id', 'INT'),
    ],
)
