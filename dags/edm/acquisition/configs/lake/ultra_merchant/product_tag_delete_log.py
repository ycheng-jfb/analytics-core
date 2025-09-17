from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="product_tag_delete_log",
    watermark_column="datetime_added",
    schema_version_prefix="v2",
    column_list=[
        Column("product_tag_delete_log_id", "INT", uniqueness=True),
        Column("product_tag_id", "INT"),
        Column("product_id", "INT"),
        Column("tag_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
