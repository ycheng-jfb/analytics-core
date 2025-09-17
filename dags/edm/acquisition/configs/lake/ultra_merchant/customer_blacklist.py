from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="customer_blacklist",
    watermark_column="datetime_added",
    schema_version_prefix="v2",
    column_list=[
        Column("customer_blacklist_id", "INT", uniqueness=True),
        Column("customer_id", "INT"),
        Column("customer_blacklist_reason_id", "INT"),
        Column("administrator_id", "INT"),
        Column("comment", "VARCHAR(255)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
