from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    table='membership_downgrade_reason',
    column_list=[
        Column('membership_downgrade_reason_id', 'INT', uniqueness=True),
        Column('store_group_id', 'INT'),
        Column('label', 'VARCHAR(100)'),
        Column('access', 'VARCHAR(15)'),
        Column('sort', 'INT'),
    ],
)
