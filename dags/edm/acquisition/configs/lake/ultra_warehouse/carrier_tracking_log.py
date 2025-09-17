from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="carrier_tracking_log",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("carrier_tracking_log_id", "INT", uniqueness=True),
        Column("carrier_tracking_id", "INT"),
        Column("event_code_id", "INT"),
        Column("datetime_event", "TIMESTAMP_NTZ(3)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("hash", "BINARY(20)"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("comment", "VARCHAR(255)"),
    ],
)
