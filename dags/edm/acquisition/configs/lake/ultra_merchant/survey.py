from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="survey",
    watermark_column="datetime_modified",
    strict_inequality=True,
    schema_version_prefix="v2",
    column_list=[
        Column("survey_id", "INT", uniqueness=True),
        Column("store_group_id", "INT"),
        Column("survey_type_id", "INT"),
        Column("label", "VARCHAR(50)"),
        Column("page_count", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("statuscode", "INT"),
        Column("title", "VARCHAR(255)"),
        Column("introduction", "VARCHAR(512)"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
