from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="psp_notification_log",
    watermark_column="datetime_modified",
    schema_version_prefix="v3",
    column_list=[
        Column("psp_notification_log_id", "INT", uniqueness=True),
        Column("payment_transaction_id", "INT"),
        Column("request_transaction_id", "VARCHAR(50)"),
        Column("response_transaction_id", "VARCHAR(50)"),
        Column("merchant_id", "VARCHAR(25)"),
        Column("transaction_type", "VARCHAR(50)"),
        Column("payment_method", "VARCHAR(25)"),
        Column("amount", "NUMBER(19, 4)"),
        Column("currency", "VARCHAR(25)"),
        Column("reason_text", "VARCHAR(255)"),
        Column("supported_operations", "VARCHAR(50)"),
        Column("environment", "VARCHAR(25)"),
        Column("success", "VARCHAR(15)"),
        Column("ip", "VARCHAR(15)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_transaction", "TIMESTAMP_NTZ(3)"),
        Column("processed", "INT"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("psp_notification_log_detail_id", "INT"),
    ],
)
