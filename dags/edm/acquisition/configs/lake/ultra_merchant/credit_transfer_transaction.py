from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="credit_transfer_transaction",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("credit_transfer_transaction_id", "INT", uniqueness=True),
        Column("store_group_id", "INT"),
        Column("store_credit_id", "INT"),
        Column("gift_certificate_id", "INT"),
        Column("credit_transfer_transaction_type_id", "INT"),
        Column("credit_transfer_provider_id", "INT"),
        Column("credit_transfer_account_id", "INT"),
        Column("amount", "NUMBER(19, 4)"),
        Column("email", "VARCHAR(100)"),
        Column("code", "VARCHAR(50)"),
        Column("foreign_transaction_id", "VARCHAR(50)"),
        Column("source_reference_number", "VARCHAR(50)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_transacted", "TIMESTAMP_NTZ(3)"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("statuscode", "INT"),
        Column("datetime_transaction", "TIMESTAMP_NTZ(3)"),
        Column("datetime_local_transaction", "TIMESTAMP_NTZ(3)"),
    ],
)
