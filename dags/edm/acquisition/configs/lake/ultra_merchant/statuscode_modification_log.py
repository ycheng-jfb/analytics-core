from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="statuscode_modification_log",
    watermark_column="datetime_added",
    initial_load_value="2020-01-15",
    schema_version_prefix="v2",
    column_list=[
        Column("statuscode_modification_log_id", "INT", uniqueness=True),
        Column("object", "VARCHAR(50)"),
        Column("object_id", "INT"),
        Column("from_statuscode", "INT"),
        Column("to_statuscode", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
