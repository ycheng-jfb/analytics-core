from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='Merlin',
    schema='dbo',
    table='Factory',
    is_full_upsert=True,
    enable_archive=True,
    schema_version_prefix='v2',
    column_list=[
        Column('oid', 'INT', uniqueness=True, source_name='OID'),
        Column('first_name', 'VARCHAR(100)', source_name='FirstName'),
        Column('last_name', 'VARCHAR(100)', source_name='LastName'),
        Column('official_factory_name', 'VARCHAR(100)', source_name='OfficialFactoryName'),
        Column('vendor', 'INT', source_name='Vendor'),
        Column('phone_number', 'VARCHAR(15)', source_name='PhoneNumber'),
        Column('factory_email_address', 'VARCHAR(100)', source_name='FactoryEmailAddress'),
        Column('contact_email_address', 'VARCHAR(100)', source_name='ContactEmailAddress'),
        Column('user_create', 'VARCHAR(36)', source_name='UserCreate'),
        Column('date_create', 'TIMESTAMP_NTZ(3)', delta_column=1, source_name='DateCreate'),
        Column('user_update', 'VARCHAR(36)', source_name='UserUpdate'),
        Column('date_update', 'TIMESTAMP_NTZ(3)', delta_column=0, source_name='DateUpdate'),
        Column('status', 'INT', source_name='Status'),
        Column('lingerie', 'BOOLEAN', source_name='Lingerie'),
    ],
)
