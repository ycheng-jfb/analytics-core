from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="order_chargeback_log",
    watermark_column="datetime_added",
    schema_version_prefix="v2",
    column_list=[
        Column("order_chargeback_log_id", "INT", uniqueness=True),
        Column("order_chargeback_id", "INT"),
        Column("administrator_id", "INT"),
        Column("orignal_statuscode", "INT"),
        Column("modified_statuscode", "INT"),
        Column("comments", "VARCHAR(200)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
