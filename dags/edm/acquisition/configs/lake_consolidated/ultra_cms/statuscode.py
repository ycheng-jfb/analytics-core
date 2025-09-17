from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    schema="ultra_cms",
    table="statuscode",
    column_list=[
        Column("statuscode", "INT", uniqueness=True),
        Column("label", "VARCHAR(50)"),
    ],
)
