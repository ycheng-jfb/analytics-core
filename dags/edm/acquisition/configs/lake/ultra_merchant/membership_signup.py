from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='membership_signup',
    schema_version_prefix='v4',
    watermark_column='datetime_modified',
    column_list=[
        Column('membership_signup_id', 'INT', uniqueness=True),
        Column('session_id', 'INT'),
        Column('membership_plan_id', 'INT'),
        Column('membership_signup_type_id', 'INT'),
        Column('membership_invitation_id', 'INT'),
        Column('customer_quiz_id', 'INT'),
        Column('customer_id', 'INT'),
        Column('shipping_address_id', 'INT'),
        Column('payment_method', 'VARCHAR(25)'),
        Column('payment_object_id', 'INT'),
        Column('payment_option_id', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('statuscode', 'INT'),
        Column('membership_type_id', 'INT'),
        Column('membership_brand_id', 'INT'),
    ],
)
