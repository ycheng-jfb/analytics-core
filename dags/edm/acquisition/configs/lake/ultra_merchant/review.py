from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='review',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('review_id', 'INT', uniqueness=True),
        Column('product_id', 'INT'),
        Column('customer_id', 'INT'),
        Column('review_template_id', 'INT'),
        Column('order_id', 'INT'),
        Column('administrator_id', 'INT'),
        Column('body', 'VARCHAR'),
        Column('recommended', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('datetime_claimed', 'TIMESTAMP_NTZ(3)'),
        Column('statuscode', 'INT'),
        Column('rating', 'DOUBLE'),
        Column('review_title', 'VARCHAR(2000)'),
    ],
)
