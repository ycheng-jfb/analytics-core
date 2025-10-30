from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='merlin',
    schema='dbo',
    table='StyleSizeScaleHdr',
    schema_version_prefix='v2',
    column_list=[
        Column('oid', 'INT', uniqueness=True, source_name='OID'),
        Column('scale', 'VARCHAR(80)', source_name='Scale'),
        Column('optimistic_lock_field', 'INT', source_name='OptimisticLockField'),
        Column('gc_record', 'INT', source_name='GCRecord'),
        Column('division', 'INT', source_name='Division'),
        Column('department', 'INT', source_name='Department'),
    ],
)
