from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="customer_link",
    watermark_column="datetime_added",
    schema_version_prefix="v2",
    column_list=[
        Column("customer_link_id", "INT", uniqueness=True),
        Column("customer_link_type_id", "INT"),
        Column("original_customer_id", "INT"),
        Column("current_customer_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
