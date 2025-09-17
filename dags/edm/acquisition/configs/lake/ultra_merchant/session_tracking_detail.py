from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="session_tracking_detail",
    watermark_column="datetime_modified",
    initial_load_value="2020-05-09 06:00:00.000",
    schema_version_prefix="v2",
    column_list=[
        Column("session_tracking_detail_id", "INT", uniqueness=True),
        Column("session_tracking_id", "INT"),
        Column("session_id", "INT"),
        Column("session_tracking_value", "VARCHAR(100)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
