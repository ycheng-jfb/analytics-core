from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="jfgc",
    schema="dbo",
    table="statuscode_group",
    schema_version_prefix="v2",
    column_list=[
        Column("statuscode_group_id", "INT", uniqueness=True),
        Column("table_name", "VARCHAR(50)"),
        Column("range_start", "INT"),
        Column("range_end", "INT"),
    ],
)
