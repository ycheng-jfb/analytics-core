from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    table="period",
    column_list=[
        Column("period_id", "INT", uniqueness=True),
        Column("type", "VARCHAR(25)"),
        Column("label", "VARCHAR(50)"),
        Column("date_period_start", "TIMESTAMP_NTZ(0)"),
        Column("date_period_end", "TIMESTAMP_NTZ(0)"),
    ],
)
