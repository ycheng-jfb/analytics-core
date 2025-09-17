from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='jf_portal',
    schema='dbo',
    table='PoChangeReasonCode',
    watermark_column='DateUpdate',
    schema_version_prefix='v3',
    column_list=[
        Column(
            'po_change_reason_code_id', 'INT', uniqueness=True, source_name='PoChangeReasonCodeId'
        ),
        Column('reason', 'VARCHAR(150)', source_name='Reason'),
        Column('description', 'VARCHAR(500)', source_name='Description'),
        Column('show', 'BOOLEAN', source_name='Show'),
        Column('date_create', 'TIMESTAMP_NTZ(3)', delta_column=1, source_name='DateCreate'),
        Column('date_update', 'TIMESTAMP_NTZ(3)', delta_column=0, source_name='DateUpdate'),
    ],
)
