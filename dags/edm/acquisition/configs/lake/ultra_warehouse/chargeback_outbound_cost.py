from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultrawarehouse',
    schema='dbo',
    table='chargeback_outbound_cost',
    schema_version_prefix='v2',
    column_list=[
        Column('chargeback_outbound_cost_id', 'INT'),
        Column('obc_year', 'VARCHAR(255)'),
        Column('obc_bu', 'VARCHAR(255)'),
        Column('obc_market', 'VARCHAR(255)'),
        Column('obc_unit_cost', 'NUMBER(19, 4)'),
    ],
)
