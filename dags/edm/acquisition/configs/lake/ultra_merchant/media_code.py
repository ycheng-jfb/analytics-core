from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="media_code",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("media_code_id", "INT", uniqueness=True),
        Column("media_code_type_id", "INT"),
        Column("media_type_id", "INT"),
        Column("media_publisher_id", "INT"),
        Column("administrator_id", "INT"),
        Column("code", "VARCHAR(255)"),
        Column("label", "VARCHAR(255)"),
        Column("enable_entry_popups", "INT"),
        Column("enable_exit_popups", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
