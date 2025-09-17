from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="[return]",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("return_id", "INT", uniqueness=True),
        Column("order_id", "INT"),
        Column("administrator_id", "INT"),
        Column("rma_id", "INT"),
        Column("refund_id", "INT"),
        Column("exchange_order_id", "INT"),
        Column("return_action_request_id", "INT"),
        Column("return_category_id", "INT"),
        Column("return_batch_id", "INT"),
        Column("return_reason_id", "INT"),
        Column("reason_comment", "VARCHAR(255)"),
        Column("contains_unidentified_items", "INT"),
        Column("process_mode", "VARCHAR(10)"),
        Column("review_by_administrator_id", "INT"),
        Column("date_added", "TIMESTAMP_NTZ(0)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("date_received", "TIMESTAMP_NTZ(0)"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("datetime_resolved", "TIMESTAMP_NTZ(3)"),
        Column("statuscode", "INT"),
        Column("datetime_transaction", "TIMESTAMP_NTZ(3)"),
        Column("datetime_local_transaction", "TIMESTAMP_NTZ(3)"),
    ],
)
