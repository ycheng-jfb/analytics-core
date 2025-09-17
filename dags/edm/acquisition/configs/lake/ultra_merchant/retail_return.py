from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="retail_return",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("retail_return_id", "INT", uniqueness=True),
        Column("retail_return_code", "VARCHAR(255)"),
        Column("order_id", "INT"),
        Column("administrator_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("statuscode", "INT"),
        Column("store_id", "INT"),
    ],
)
