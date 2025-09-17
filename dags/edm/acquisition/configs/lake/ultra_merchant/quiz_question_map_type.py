from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="quiz_question_map_type",
    watermark_column="datetime_added",
    schema_version_prefix="v2",
    column_list=[
        Column("quiz_question_map_type_id", "INT", uniqueness=True),
        Column("store_group_id", "INT"),
        Column("type", "VARCHAR(25)"),
        Column("label", "VARCHAR(50)"),
        Column("parent_tag_id", "INT"),
        Column("customer_detail_name", "VARCHAR(50)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
