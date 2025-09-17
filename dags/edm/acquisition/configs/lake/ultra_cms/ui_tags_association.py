from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultracms",
    schema="dbo",
    table="ui_tags_association",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("ui_tags_association_id", "INT", uniqueness=True),
        Column("ui_tags_id", "INT"),
        Column("table_name", "VARCHAR(50)"),
        Column("table_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("isactive", "INT"),
    ],
)
