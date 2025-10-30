from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='jfgc',
    schema='dbo',
    table='credit_source_type',
    schema_version_prefix='v2',
    column_list=[
        Column('credit_source_type_id', 'INT', uniqueness=True),
        Column('code', 'VARCHAR(50)'),
        Column('label', 'VARCHAR(100)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
