from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="visitor_session",
    watermark_column="datetime_modified",
    initial_load_value="2020-01-15",
    partition_cols=["visitor_session_id", "visitor_session_hash_id"],
    schema_version_prefix="v2",
    column_list=[
        Column("visitor_session_id", "INT", uniqueness=True),
        Column("visitor_session_hash_id", "INT"),
        Column("visitor_id", "INT"),
        Column("session_id", "INT"),
        Column("datetime_expired", "TIMESTAMP_NTZ(3)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
