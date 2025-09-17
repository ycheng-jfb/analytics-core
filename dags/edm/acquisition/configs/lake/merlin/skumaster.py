from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='merlin',
    schema='dbo',
    table='skumaster',
    watermark_column='DateUpdate',
    schema_version_prefix='v2',
    column_list=[
        Column('oid', 'INT', uniqueness=True, source_name='OID'),
        Column('style', 'INT', source_name='Style'),
        Column('color', 'INT', source_name='Color'),
        Column('style_size_scale_dtl', 'INT', source_name='StyleSizeScaleDtl'),
        Column('v_sku', 'VARCHAR(30)', source_name='vSku'),
        Column('optimistic_lock_field', 'INT', source_name='OptimisticLockField'),
        Column('gc_record', 'INT', source_name='GCRecord'),
        Column('date_create', 'TIMESTAMP_NTZ(3)', delta_column=1, source_name='DateCreate'),
        Column('date_update', 'TIMESTAMP_NTZ(3)', delta_column=0, source_name='DateUpdate'),
        Column('upc', 'VARCHAR(12)', source_name='UPC'),
        Column('ean', 'VARCHAR(13)', source_name='EAN'),
    ],
)
