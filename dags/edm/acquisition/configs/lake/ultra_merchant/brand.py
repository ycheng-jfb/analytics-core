from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='brand',
    watermark_column='datetime_added',
    schema_version_prefix='v2',
    column_list=[
        Column('brand_id', 'INT', uniqueness=True),
        Column('store_group_id', 'INT'),
        Column('code', 'VARCHAR(25)'),
        Column('label', 'VARCHAR(100)'),
        Column('meta_keywords', 'VARCHAR(255)'),
        Column('meta_description', 'VARCHAR(255)'),
        Column('disclaimer', 'VARCHAR(255)'),
        Column('country_code', 'VARCHAR(2)'),
        Column('image_file_type', 'VARCHAR(25)'),
        Column('call_for_price', 'INT'),
        Column('international_is_ok', 'INT'),
        Column('no_online_sales', 'INT'),
        Column('msrp_required', 'INT'),
        Column('login_required', 'INT'),
        Column('is_internal', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('statuscode', 'INT'),
        Column('new_membership_promo_allowed', 'INT'),
    ],
)
