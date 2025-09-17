from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="cycle_count_criteria",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("cycle_count_criteria_id", "INT", uniqueness=True),
        Column("cycle_count_id", "INT"),
        Column("type_code_id", "INT"),
        Column("value", "VARCHAR(255)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
