from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="modification_log",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("modification_log_id", "INT", uniqueness=True),
        Column("object", "VARCHAR(50)"),
        Column("object_id", "INT"),
        Column("field", "VARCHAR(50)"),
        Column(
            "old_value",
            "VARCHAR",
            source_name="convert(VARCHAR, old_value, 23) AS old_value",
        ),
        Column(
            "new_value",
            "VARCHAR",
            source_name="convert(VARCHAR, new_value, 23) AS new_value",
        ),
        Column("administrator_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
