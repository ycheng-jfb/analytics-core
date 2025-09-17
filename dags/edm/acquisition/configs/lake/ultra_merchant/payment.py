from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="payment",
    watermark_column="datetime_modified",
    initial_load_value="2020-01-14",
    schema_version_prefix="v2",
    column_list=[
        Column("payment_id", "INT", uniqueness=True),
        Column("order_id", "INT"),
        Column("payment_method", "VARCHAR(25)"),
        Column("payment_object_id", "INT"),
        Column("auth_payment_transaction_id", "INT"),
        Column("capture_payment_transaction_id", "INT"),
        Column("payment_number", "INT"),
        Column("amount", "NUMBER(19, 4)"),
        Column("original_amount", "NUMBER(19, 4)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("date_payment_originally_due", "TIMESTAMP_NTZ(0)"),
        Column("date_payment_due", "TIMESTAMP_NTZ(0)"),
        Column("statuscode", "INT"),
        Column("datetime_transaction", "TIMESTAMP_NTZ(3)"),
        Column("datetime_local_transaction", "TIMESTAMP_NTZ(3)"),
        Column("subtotal", "NUMBER(19, 4)"),
        Column("shipping", "NUMBER(19, 4)"),
        Column("tax", "NUMBER(19, 4)"),
    ],
)
