from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="session_migration_log_detail",
    watermark_column="datetime_added",
    schema_version_prefix="v2",
    column_list=[
        Column("session_migration_log_detail_id", "INT", uniqueness=True),
        Column("session_migration_log_id", "INT"),
        Column("table_name", "VARCHAR(100)"),
        Column("column_name", "VARCHAR(100)"),
        Column("column_value", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
