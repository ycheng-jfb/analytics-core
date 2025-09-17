from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="refund_line",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("refund_line_id", "INT", uniqueness=True),
        Column("refund_id", "INT"),
        Column("order_line_id", "INT"),
        Column("refund_amount", "NUMBER(19, 4)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
