from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='membership_plan_membership_brand',
    watermark_column='datetime_added',
    column_list=[
        Column('membership_plan_membership_brand_id', 'INT', uniqueness=True),
        Column('membership_plan_id', 'INT'),
        Column('membership_brand_id', 'INT'),
        Column('store_id', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
