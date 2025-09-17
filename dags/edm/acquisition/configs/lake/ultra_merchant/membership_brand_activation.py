from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='membership_brand_activation',
    watermark_column='datetime_modified',
    column_list=[
        Column('membership_brand_activation_id', 'INT', uniqueness=True),
        Column('order_id', 'INT'),
        Column('membership_level_group_modification_log_id', 'INT'),
        Column('membership_brand_id', 'INT'),
        Column('membership_id', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_activated', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
