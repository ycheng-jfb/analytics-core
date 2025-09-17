from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="order_delete_log",
    watermark_column="datetime_added",
    schema_version_prefix="v2",
    column_list=[
        Column("order_delete_log_id", "INT", uniqueness=True),
        Column("order_id", "INT"),
        Column("store_id", "INT"),
        Column("store_group_id", "INT"),
        Column("customer_id", "INT"),
        Column("payment_method", "VARCHAR(25)"),
        Column("code", "VARCHAR(36)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
