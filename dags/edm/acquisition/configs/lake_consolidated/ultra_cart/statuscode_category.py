from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    schema="ultra_cart",
    table="statuscode_category",
    column_list=[
        Column("statuscode_category_id", "INT", uniqueness=True),
        Column("label", "VARCHAR(50)"),
        Column("range_start", "INT"),
        Column("range_end", "INT"),
    ],
)
