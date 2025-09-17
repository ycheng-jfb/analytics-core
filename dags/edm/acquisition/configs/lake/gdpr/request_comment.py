from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="gdpr",
    schema="dbo",
    table="request_comment",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("request_comment_id", "INT", uniqueness=True),
        Column("request_id", "INT"),
        Column("administrator_id", "INT"),
        Column("comment", "VARCHAR(8000)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
