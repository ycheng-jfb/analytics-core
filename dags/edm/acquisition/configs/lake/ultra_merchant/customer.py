from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='customer',
    watermark_column='datetime_modified',
    initial_load_value='2020-01-17',
    schema_version_prefix='v2',
    column_list=[
        Column('customer_id', 'INT', uniqueness=True),
        Column('store_group_id', 'INT'),
        Column('store_id', 'INT'),
        Column('customer_type_id', 'INT'),
        Column('customer_key', 'VARCHAR(36)'),
        Column('username', 'VARCHAR(75)'),
        Column('password', 'VARCHAR(150)'),
        Column('email', 'VARCHAR(75)'),
        Column('firstname', 'VARCHAR(25)'),
        Column('lastname', 'VARCHAR(25)'),
        Column('name', 'VARCHAR(50)'),
        Column('company', 'VARCHAR(100)'),
        Column('default_address_id', 'INT'),
        Column('default_discount_id', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('statuscode', 'INT'),
        Column('password_salt', 'VARCHAR(25)'),
        Column('ph_method', 'VARCHAR(25)'),
    ],
)
