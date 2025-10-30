from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultracms',
    schema='dbo',
    table='test_framework_data',
    watermark_column='datetime_modified',
    schema_version_prefix='v3',
    column_list=[
        Column('test_famework_data_id', 'INT', uniqueness=True),
        Column('test_framework_id', 'INT'),
        Column('object', 'VARCHAR(50)'),
        Column('value', 'VARCHAR(50)'),
        Column('comments', 'VARCHAR(250)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
