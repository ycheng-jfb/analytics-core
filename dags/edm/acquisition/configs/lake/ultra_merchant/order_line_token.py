from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="order_line_token",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("order_line_token_id", "INT", uniqueness=True),
        Column("order_id", "INT"),
        Column("order_line_id", "INT"),
        Column("membership_token_id", "INT"),
        Column("purchase_price", "NUMBER(19, 4)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
