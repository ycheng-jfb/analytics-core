from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultracms',
    schema='dbo',
    table='navigation_configurations',
    schema_version_prefix='v2',
    column_list=[
        Column('navigation_configuration_id', 'INT', uniqueness=True),
        Column('container_id', 'INT'),
        Column('start_date', 'TIMESTAMP_NTZ(3)'),
        Column('end_date', 'TIMESTAMP_NTZ(3)'),
        Column('label', 'VARCHAR(255)'),
        Column('ga_category', 'VARCHAR(255)'),
        Column('config_version', 'INT'),
    ],
)
