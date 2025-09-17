from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="membership_product_wait_list",
    schema_version_prefix="v3",
    watermark_column="datetime_modified",
    column_list=[
        Column("membership_product_wait_list_id", "INT", uniqueness=True),
        Column("membership_id", "INT"),
        Column("product_id", "INT"),
        Column("membership_product_wait_list_type_id", "INT"),
        Column("action_attempts", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("date_expires", "TIMESTAMP_NTZ(0)"),
        Column("datetime_last_action", "TIMESTAMP_NTZ(3)"),
        Column("date_last_action", "TIMESTAMP_NTZ(0)"),
        Column("statuscode", "INT"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("credit_allowed", "BOOLEAN"),
    ],
)
