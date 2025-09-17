from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="case_classification",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("case_classification_id", "INT", uniqueness=True),
        Column("case_id", "INT"),
        Column("case_flag_id", "INT"),
        Column("case_flag_type_id", "INT"),
        Column("administrator_id", "INT"),
        Column("comment", "VARCHAR(100)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
