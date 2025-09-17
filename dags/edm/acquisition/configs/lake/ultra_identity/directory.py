from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultraidentity",
    schema="dbo",
    table="directory",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("directory_id", "INT", uniqueness=True),
        Column("label", "VARCHAR(255)"),
        Column("type_code_id", "INT"),
        Column("is_master", "BOOLEAN"),
        Column("company_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("status_code_id", "INT"),
    ],
)
