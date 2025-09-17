from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="box_completion",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("box_completion_id", "INT", uniqueness=True),
        Column("box_id", "INT"),
        Column("box_completion_disposition_id", "INT"),
        Column("administrator_id", "INT"),
        Column("disposition_comment", "VARCHAR(255)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
