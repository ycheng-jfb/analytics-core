from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="administrator_store_group",
    column_list=[
        Column("administrator_id", "INT", uniqueness=True),
        Column("store_group_id", "INT", uniqueness=True),
        Column("granter_administrator_id", "INT"),
        Column("is_default", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_added",
)
