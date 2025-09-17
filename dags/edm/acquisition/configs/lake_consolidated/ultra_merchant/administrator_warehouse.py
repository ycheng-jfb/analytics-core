from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="administrator_warehouse",
    column_list=[
        Column("administrator_id", "INT", uniqueness=True),
        Column("warehouse_id", "INT", uniqueness=True),
        Column("granter_administrator_id", "INT"),
        Column("badge_id", "VARCHAR(50)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_added",
)
