from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultrawarehouse',
    schema='dbo',
    table='cycle_count_data',
    schema_version_prefix='v2',
    column_list=[
        Column('cycle_count_location_id', 'INT', uniqueness=True),
        Column('sequence', 'INT', uniqueness=True),
        Column('data', 'VARCHAR', uniqueness=True),
        Column('result', 'VARCHAR', uniqueness=True),
    ],
)
