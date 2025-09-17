from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ssrs_reports",
    schema="gsc",
    table="pulse_comments",
    watermark_column="meta_modified_datetime",
    schema_version_prefix="v2",
    column_list=[
        Column("row_key", "BIGINT", uniqueness=True),
        Column("comment", "VARCHAR(1000)"),
        Column("comment_command", "VARCHAR(50)"),
        Column("comment_subject", "VARCHAR(50)"),
        Column("comment_type", "VARCHAR(50)"),
        Column(
            "datetime_added", "TIMESTAMP_NTZ(3)", source_name="meta_create_datetime"
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
            delta_column=True,
            source_name="meta_modified_datetime",
        ),
    ],
)
