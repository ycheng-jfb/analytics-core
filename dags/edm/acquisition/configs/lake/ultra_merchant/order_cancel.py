from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="order_cancel",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("order_cancel_id", "INT", uniqueness=True),
        Column("order_id", "INT"),
        Column("administrator_id", "INT"),
        Column("order_cancel_reason_id", "INT"),
        Column("reason_comment", "VARCHAR(255)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("datetime_resolved", "TIMESTAMP_NTZ(3)"),
        Column("statuscode", "INT"),
    ],
)
