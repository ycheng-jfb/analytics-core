from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultracms',
    schema='dbo',
    table='test_framework_xfa',
    watermark_column='datetime_modified',
    schema_version_prefix='v3',
    column_list=[
        Column('test_framework_xfa_id', 'INT', uniqueness=True),
        Column('store_group_id', 'INT'),
        Column('label', 'VARCHAR(50)'),
        Column('xfa', 'VARCHAR(50)'),
        Column('xfa_sort', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
