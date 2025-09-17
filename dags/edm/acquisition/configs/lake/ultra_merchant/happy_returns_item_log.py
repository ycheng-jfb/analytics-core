from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="happy_returns_item_log",
    watermark_column="datetime_added",
    column_list=[
        Column("happy_returns_item_log_id", "INT", uniqueness=True),
        Column("rma_id", "INT"),
        Column("order_line_id", "INT"),
        Column("action", "VARCHAR(50)"),
        Column("happy_returns_rma_id", "VARCHAR(50)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
