from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='jf_portal',
    schema='dbo',
    table='IncoTerm',
    watermark_column='DateUpdate',
    schema_version_prefix='v3',
    column_list=[
        Column('inco_term_id', 'INT', uniqueness=True, source_name='IncoTermId'),
        Column('name', 'VARCHAR(50)', source_name='Name'),
        Column('code', 'VARCHAR(20)', source_name='Code'),
        Column('geo', 'VARCHAR(20)', source_name='GEO'),
        Column('date_create', 'TIMESTAMP_NTZ(3)', delta_column=1, source_name='DateCreate'),
        Column('date_update', 'TIMESTAMP_NTZ(3)', delta_column=0, source_name='DateUpdate'),
    ],
)
