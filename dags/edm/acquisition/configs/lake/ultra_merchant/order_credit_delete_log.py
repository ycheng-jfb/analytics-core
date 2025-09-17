from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="order_credit_delete_log",
    watermark_column="datetime_added",
    schema_version_prefix="v2",
    column_list=[
        Column("order_credit_delete_log_id", "INT", uniqueness=True),
        Column("order_credit_id", "INT"),
        Column("order_id", "INT"),
        Column("gift_certificate_id", "INT"),
        Column("store_credit_id", "INT"),
        Column("amount", "NUMBER(19, 4)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
