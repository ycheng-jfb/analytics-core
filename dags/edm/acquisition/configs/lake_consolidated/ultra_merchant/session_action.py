from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    table='session_action',
    column_list=[
        Column('session_action_id', 'INT', uniqueness=True),
        Column('label', 'VARCHAR(50)'),
    ],
)
