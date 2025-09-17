from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='administrator',
    watermark_column='date_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('administrator_id', 'INT', uniqueness=True),
        Column('administrator_key', 'VARCHAR(36)'),
        Column('login', 'VARCHAR(25)'),
        Column('password', 'VARCHAR(150)'),
        Column('firstname', 'VARCHAR(25)'),
        Column('lastname', 'VARCHAR(25)'),
        Column('approval_code', 'VARCHAR(15)'),
        Column('active', 'INT'),
        Column('phone_ext', 'INT'),
        Column('date_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('date_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('password_salt', 'VARCHAR(25)'),
        Column('ph_method', 'VARCHAR(25)'),
        Column('active_directory_guid', 'VARCHAR(50)', source_name='active_directory_GUID'),
        Column('invalid_login_attempts', 'INT'),
        Column('datetime_last_login_attempt', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_password_expires', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_approval_code_expires', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_locked', 'TIMESTAMP_NTZ(3)'),
    ],
)
