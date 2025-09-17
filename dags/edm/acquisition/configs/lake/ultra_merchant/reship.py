from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="reship",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("reship_id", "INT", uniqueness=True),
        Column("original_order_id", "INT"),
        Column("reship_order_id", "INT"),
        Column("reship_reason_id", "INT"),
        Column("reason_comment", "VARCHAR(255)"),
        Column("date_added", "TIMESTAMP_NTZ(0)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
