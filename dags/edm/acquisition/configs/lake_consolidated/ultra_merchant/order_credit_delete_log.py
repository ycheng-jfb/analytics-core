from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="order_credit_delete_log",
    column_list=[
        Column("order_credit_delete_log_id", "INT", uniqueness=True, key=True),
        Column("order_credit_id", "INT", key=True),
        Column("order_id", "INT"),
        Column("gift_certificate_id", "INT"),
        Column("store_credit_id", "INT"),
        Column("amount", "NUMBER(19, 4)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_added",
)
