from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='jfgc',
    schema='dbo',
    table='statuscode',
    schema_version_prefix='v2',
    column_list=[Column('statuscode', 'INT', uniqueness=True), Column('label', 'VARCHAR(50)')],
)
