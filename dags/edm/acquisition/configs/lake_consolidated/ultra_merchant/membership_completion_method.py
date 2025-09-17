from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    table="membership_completion_method",
    column_list=[
        Column("membership_completion_method_id", "INT", uniqueness=True),
        Column("label", "VARCHAR(50)"),
    ],
)
