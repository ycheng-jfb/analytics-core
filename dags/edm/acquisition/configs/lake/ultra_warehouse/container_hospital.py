from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultrawarehouse',
    schema='dbo',
    table='container_hospital',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('container_hospital_id', 'INT', uniqueness=True),
        Column('container_id', 'INT'),
        Column('warehouse_id', 'INT'),
        Column('lpn_count', 'INT'),
        Column('administrator_id', 'INT'),
        Column('closed_administrator_id', 'INT'),
        Column('datetime_closed', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('status_code_id', 'INT'),
    ],
)
