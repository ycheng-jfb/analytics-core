from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultrawarehouse',
    schema='dbo',
    table='cycle_count',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('cycle_count_id', 'INT', uniqueness=True),
        Column('type_code_id', 'INT'),
        Column('warehouse_id', 'INT'),
        Column('label', 'VARCHAR(255)'),
        Column('description', 'VARCHAR(512)'),
        Column('datetime_included', 'TIMESTAMP_NTZ(3)'),
        Column('completed_locations', 'INT'),
        Column('total_locations', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('status_code_id', 'INT'),
    ],
)
