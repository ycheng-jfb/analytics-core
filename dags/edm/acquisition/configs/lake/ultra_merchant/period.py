from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='period',
    schema_version_prefix='v2',
    column_list=[
        Column('period_id', 'INT', uniqueness=True),
        Column('type', 'VARCHAR(25)'),
        Column('label', 'VARCHAR(50)'),
        Column('date_period_start', 'TIMESTAMP_NTZ(0)'),
        Column('date_period_end', 'TIMESTAMP_NTZ(0)'),
    ],
)
