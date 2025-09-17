from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='Merlin',
    schema='dbo',
    table='Country',
    schema_version_prefix='v2',
    column_list=[
        Column('iso', 'VARCHAR(2)', uniqueness=True, source_name='ISO'),
        Column('name', 'VARCHAR(80)', source_name='Name'),
        Column('nice_name', 'VARCHAR(80)', source_name='NiceName'),
        Column('is_o3', 'VARCHAR(3)', source_name='ISO3'),
        Column('num_code', 'INT', source_name='NumCode'),
        Column('phone_code', 'INT', source_name='PhoneCode'),
        Column('is_country_of_origin', 'BOOLEAN', source_name='IsCountryOfOrigin'),
        Column('optimistic_lock_field', 'INT', source_name='OptimisticLockField'),
        Column('gc_record', 'INT', source_name='GCRecord'),
        Column('culture', 'VARCHAR(100)', source_name='Culture'),
    ],
)
