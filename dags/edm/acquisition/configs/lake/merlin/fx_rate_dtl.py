from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='Merlin',
    schema='dbo',
    table='FxRateDtl',
    watermark_column='DateUpdate',
    schema_version_prefix='v2',
    column_list=[
        Column('oid', 'INT', uniqueness=True, source_name='OID'),
        Column('ask', 'DOUBLE', source_name='Ask'),
        Column('bid', 'DOUBLE', source_name='Bid'),
        Column('exch_date', 'TIMESTAMP_NTZ(3)', source_name='ExchDate'),
        Column('fx_rate_currency', 'INT', source_name='FxRateCurrency'),
        Column('fx_rate_hdr', 'INT', source_name='FxRateHdr'),
        Column('gp_apply_date', 'TIMESTAMP_NTZ(3)', source_name='GpApplyDate'),
        Column('m_c00100_dex_row_id', 'INT', source_name='MC00100DexRowId'),
        Column('optimistic_lock_field', 'INT', source_name='OptimisticLockField'),
        Column('gc_record', 'INT', source_name='GCRecord'),
        Column('date_create', 'TIMESTAMP_NTZ(3)', delta_column=1, source_name='DateCreate'),
        Column('date_update', 'TIMESTAMP_NTZ(3)', delta_column=0, source_name='DateUpdate'),
    ],
)
