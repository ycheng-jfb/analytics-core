from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="cart_warehouse",
    watermark_column="datetime_modified",
    schema_version_prefix="v3",
    column_list=[
        Column("cart_warehouse_id", "INT", uniqueness=True),
        Column("cart_id", "INT"),
        Column("warehouse_id", "INT"),
        Column("statuscode", "INT"),
        Column("administrator_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("datetime_expired", "TIMESTAMP_NTZ(3)"),
    ],
)
