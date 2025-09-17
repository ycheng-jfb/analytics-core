from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="chargeback_import_transaction",
    watermark_column="datetime_added",
    schema_version_prefix="v2",
    column_list=[
        Column("chargeback_import_transaction_id", "INT", uniqueness=True),
        Column("chargeback_import_batch_detail_id", "INT"),
        Column("customer_id", "INT"),
        Column("order_id", "INT"),
        Column("payment_transaction_id", "INT"),
        Column("is_duplicate", "BOOLEAN"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("statuscode", "VARCHAR(30)"),
    ],
)
