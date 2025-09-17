from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    table="language",
    column_list=[
        Column("language_id", "INT", uniqueness=True),
        Column("code", "VARCHAR(5)"),
        Column("label", "VARCHAR(50)"),
    ],
)
