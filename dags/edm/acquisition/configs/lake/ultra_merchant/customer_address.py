from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="customer_address",
    watermark_column="datetime_added",
    schema_version_prefix="v2",
    column_list=[
        Column("customer_address_id", "INT", uniqueness=True),
        Column("customer_id", "INT"),
        Column("address_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
