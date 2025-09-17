from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="order_delete_log",
    column_list=[
        Column("order_delete_log_id", "INT", uniqueness=True, key=True),
        Column("order_id", "INT"),
        Column("store_id", "INT"),
        Column("store_group_id", "INT"),
        Column("customer_id", "INT"),
        Column("payment_method", "VARCHAR(25)"),
        Column("code", "VARCHAR(36)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_added",
)
