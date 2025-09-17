from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='membership_settings',
    column_list=[
        Column('membership_settings_id', 'INT', uniqueness=True),
        Column('store_id', 'INT'),
        Column('store_domain_type_id', 'INT'),
        Column('dm_site_id', 'INT'),
        Column('membership_plan_id', 'INT'),
        Column('membership_signup_type_id', 'INT'),
        Column('quiz_id', 'INT'),
        Column('weight', 'INT'),
        Column('invitation_only', 'INT'),
        Column('dm_traffic_default', 'INT'),
        Column('session_detail', 'VARCHAR(4000)'),
        Column('membership_type_id', 'INT'),
        Column('membership_brand_id', 'INT'),
    ],
)
