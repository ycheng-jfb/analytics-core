from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='dm_gateway',
    watermark_column='datetime_modified',
    schema_version_prefix='v3',
    column_list=[
        Column('dm_gateway_id', 'INT', uniqueness=True),
        Column('store_group_id', 'INT'),
        Column('dm_gateway_type_id', 'INT'),
        Column('store_id', 'INT'),
        Column('offer_category_id', 'INT'),
        Column('code', 'VARCHAR(25)'),
        Column('label', 'VARCHAR(50)'),
        Column('description', 'VARCHAR(255)'),
        Column('redirect_dm_gateway_id', 'INT'),
        Column('statuscode', 'INT'),
        Column('dm_gateway_sub_type_id', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('membership_brand_id', 'INT'),
    ],
)
