from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="jfgc",
    schema="dbo",
    table="gift_card_transaction",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("gift_card_transaction_id", "INT", uniqueness=True),
        Column("merchant_id", "INT"),
        Column("gift_card_id", "INT"),
        Column("transaction_type", "VARCHAR(50)"),
        Column("credit_source_type_id", "INT"),
        Column("amount", "NUMBER(19, 4)"),
        Column("reference_number", "VARCHAR(50)"),
        Column("reference_source_type", "VARCHAR(50)"),
        Column("email", "VARCHAR(100)"),
        Column("gift_card_code", "VARCHAR(50)"),
        Column("error_message", "VARCHAR(255)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("datetime_originally_issued", "TIMESTAMP_NTZ(3)"),
        Column("statuscode", "INT"),
    ],
)
