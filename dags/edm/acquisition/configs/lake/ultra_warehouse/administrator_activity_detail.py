from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultrawarehouse',
    schema='dbo',
    table='administrator_activity_detail',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('administrator_activity_detail_id', 'INT', uniqueness=True),
        Column('administrator_id', 'INT'),
        Column('object', 'VARCHAR(35)'),
        Column('object_id', 'INT'),
        Column('detail_1', 'VARCHAR(255)'),
        Column('detail_2', 'VARCHAR(255)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
