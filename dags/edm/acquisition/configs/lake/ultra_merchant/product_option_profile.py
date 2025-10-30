from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='product_option_profile',
    watermark_column='datetime_added',
    column_list=[
        Column('product_option_profile_id', 'INT', uniqueness=True),
        Column('store_group_id', 'INT'),
        Column('gender', 'VARCHAR(10)'),
        Column('label', 'VARCHAR(50)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
