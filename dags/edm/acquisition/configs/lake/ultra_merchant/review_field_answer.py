from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="review_field_answer",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("review_field_answer_id", "INT", uniqueness=True),
        Column("review_id", "INT"),
        Column("review_template_field_id", "INT"),
        Column("review_template_field_answer_id", "INT"),
        Column("value", "VARCHAR(2000)"),
        Column("comment", "VARCHAR(512)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
