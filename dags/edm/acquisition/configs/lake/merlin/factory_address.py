from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='Merlin',
    schema='dbo',
    table='FactoryAddress',
    watermark_column='oid',
    initial_load_value='0',
    strict_inequality=True,
    enable_archive=False,
    schema_version_prefix='v2',
    column_list=[
        Column('oid', 'INT', uniqueness=True, delta_column=True, source_name='OID'),
        Column('street_number', 'VARCHAR(100)', source_name='StreetNumber'),
        Column('street_name', 'VARCHAR(100)', source_name='StreetName'),
        Column('city', 'VARCHAR(100)', source_name='City'),
        Column('state', 'INT', source_name='State'),
        Column('zip_code', 'VARCHAR(100)', source_name='ZipCode'),
        Column('country', 'VARCHAR(2)', source_name='Country'),
        Column('factory', 'INT', source_name='Factory'),
        Column('optimistic_lock_field', 'INT', source_name='OptimisticLockField'),
        Column('gc_record', 'INT', source_name='GCRecord'),
    ],
)
