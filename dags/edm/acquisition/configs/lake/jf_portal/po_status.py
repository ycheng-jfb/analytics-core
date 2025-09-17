from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='jf_portal',
    schema='dbo',
    table='PoStatus',
    watermark_column='DateUpdate',
    schema_version_prefix='v3',
    column_list=[
        Column('po_status_id', 'INT', uniqueness=True, source_name='PoStatusId'),
        Column('description', 'VARCHAR(50)', source_name='Description'),
        Column('date_create', 'TIMESTAMP_NTZ(3)', delta_column=1, source_name='DateCreate'),
        Column('date_update', 'TIMESTAMP_NTZ(3)', delta_column=0, source_name='DateUpdate'),
    ],
)
