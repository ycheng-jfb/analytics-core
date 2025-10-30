from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='Merlin',
    schema='dbo',
    table='EnumCode',
    watermark_column='oid',
    initial_load_value='0',
    strict_inequality=True,
    enable_archive=False,
    schema_version_prefix='v2',
    column_list=[
        Column('oid', 'INT', uniqueness=True, delta_column=True, source_name='OID'),
        Column('var_name', 'VARCHAR(50)', source_name='VarName'),
        Column('var_value', 'INT', source_name='VarValue'),
        Column('enum_name', 'VARCHAR(50)', source_name='EnumName'),
        Column('enum_full_name', 'VARCHAR', source_name='EnumFullName'),
        Column('optimistic_lock_field', 'INT', source_name='OptimisticLockField'),
        Column('gc_record', 'INT', source_name='GCRecord'),
    ],
)
