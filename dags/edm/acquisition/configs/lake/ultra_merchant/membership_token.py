from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="membership_token",
    schema_version_prefix="v4",
    watermark_column="datetime_modified",
    column_list=[
        Column("membership_token_id", "INT", uniqueness=True),
        Column("membership_id", "INT"),
        Column("membership_billing_id", "INT"),
        Column("order_id", "INT"),
        Column("administrator_id", "INT"),
        Column("membership_token_reason_id", "INT"),
        Column("reason_comment", "VARCHAR(255)"),
        Column("code", "VARCHAR(25)"),
        Column("purchase_price", "NUMBER(19, 4)"),
        Column("currency_code", "VARCHAR(3)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("date_expires", "TIMESTAMP_NTZ(0)"),
        Column("statuscode", "INT"),
        Column("extension_months", "INT"),
    ],
)
