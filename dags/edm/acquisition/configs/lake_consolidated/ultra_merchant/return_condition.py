from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    table='return_condition',
    column_list=[
        Column('return_condition_id', 'INT', uniqueness=True),
        Column('code', 'VARCHAR(25)'),
        Column('label', 'VARCHAR(50)'),
        Column('description', 'VARCHAR(255)'),
    ],
)
