from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="session_log",
    watermark_column="datetime_modified",
    initial_load_value="2020-05-10 06:00:00.000",
    partition_cols=["session_log_id", "session_log_hash_id"],
    schema_version_prefix="v2",
    column_list=[
        Column("session_log_id", "INT", uniqueness=True),
        Column("session_log_hash_id", "INT"),
        Column("session_id", "INT"),
        Column("session_action_id", "INT"),
        Column("object", "VARCHAR(50)"),
        Column("object_id", "INT"),
        Column("action_count", "INT"),
        Column("store_domain_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
