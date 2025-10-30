from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='merlin',
    schema='dbo',
    table='StyleMSRP',
    schema_version_prefix='v2',
    column_list=[
        Column('oid', 'INT', uniqueness=True, source_name='OID'),
        Column('style', 'INT', source_name='Style'),
        Column('msrp', 'NUMBER(19, 4)', source_name='MSRP'),
        Column('country', 'VARCHAR(2)', source_name='Country'),
        Column('display_msrp', 'VARCHAR(100)', source_name='DisplayMSRP'),
        Column('vip_price', 'VARCHAR(100)', source_name='VipPrice'),
        Column('optimistic_lock_field', 'INT', source_name='OptimisticLockField'),
        Column('gc_record', 'INT', source_name='GCRecord'),
        Column('currency', 'INT', source_name='Currency'),
    ],
)
