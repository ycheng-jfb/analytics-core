from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="fraud_customer",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("fraud_customer_id", "INT", uniqueness=True),
        Column("customer_id", "INT"),
        Column("fraud_customer_reason_id", "INT"),
        Column("comment", "VARCHAR(255)"),
        Column("administrator_id", "INT"),
        Column("remove_comment", "VARCHAR(255)"),
        Column("remove_administrator_id", "INT"),
        Column("datetime_remove", "TIMESTAMP_NTZ(3)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("statuscode", "INT"),
    ],
)
