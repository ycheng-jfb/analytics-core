from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="customer_last_event",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("customer_last_event_id", "INT", uniqueness=True),
        Column("customer_id", "INT"),
        Column("customer_last_event_type_id", "INT"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("datetime_previous_login", "TIMESTAMP_NTZ(3)"),
    ],
)
