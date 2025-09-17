from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    table="promo_classification",
    column_list=[
        Column("promo_classification_id", "INT", uniqueness=True),
        Column("label", "VARCHAR(50)"),
        Column("parent_promo_classification_id", "INT"),
        Column("sort", "INT"),
    ],
)
