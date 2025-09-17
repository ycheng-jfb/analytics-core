from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultrawarehouse',
    schema='dbo',
    table='chargeback_return_cost',
    schema_version_prefix='v2',
    column_list=[
        Column('chargeback_return_cost_id', 'INT'),
        Column('rtc_bu', 'VARCHAR(255)'),
        Column('rtc_market', 'VARCHAR(255)'),
        Column('rtc_unit_cost', 'NUMBER(19, 4)'),
    ],
)
