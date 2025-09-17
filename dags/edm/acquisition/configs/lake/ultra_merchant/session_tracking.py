from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="session_tracking",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("session_tracking_id", "INT", uniqueness=True),
        Column("code", "VARCHAR(50)"),
        Column("label", "VARCHAR(50)"),
        Column("is_unique", "BOOLEAN"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
