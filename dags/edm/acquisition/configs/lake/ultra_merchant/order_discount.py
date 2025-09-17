from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='order_discount',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('order_discount_id', 'INT', uniqueness=True),
        Column('order_id', 'INT'),
        Column('discount_type_id', 'INT'),
        Column('discount_id', 'INT'),
        Column('promo_id', 'INT'),
        Column('applied_to', 'VARCHAR(15)'),
        Column('amount', 'NUMBER(19, 4)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
