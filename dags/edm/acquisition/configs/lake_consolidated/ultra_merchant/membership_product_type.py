from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    table="membership_product_type",
    column_list=[
        Column("membership_product_type_id", "INT", uniqueness=True),
        Column("label", "VARCHAR(50)"),
    ],
)
