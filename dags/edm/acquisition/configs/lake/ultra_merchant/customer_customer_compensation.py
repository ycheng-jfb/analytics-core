from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="customer_customer_compensation",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("customer_customer_compensation_id", "INT", uniqueness=True),
        Column("customer_id", "INT"),
        Column("customer_compensation_id", "INT"),
        Column("administrator_id", "INT"),
        Column("approved_administrator_id", "INT"),
        Column("object", "VARCHAR(50)"),
        Column("object_id", "INT"),
        Column("previous_balance", "DOUBLE"),
        Column("balance", "DOUBLE"),
        Column("customer_log_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("customer_compensation_vip_reason_id", "INT"),
    ],
)
