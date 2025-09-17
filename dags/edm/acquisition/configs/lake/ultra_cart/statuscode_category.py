from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultracart",
    schema="dbo",
    table="statuscode_category",
    schema_version_prefix="v2",
    column_list=[
        Column("statuscode_category_id", "INT", uniqueness=True),
        Column("label", "VARCHAR(50)"),
        Column("range_start", "INT"),
        Column("range_end", "INT"),
    ],
)
