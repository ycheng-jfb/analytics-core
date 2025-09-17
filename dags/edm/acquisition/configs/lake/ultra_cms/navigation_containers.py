from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultracms",
    schema="dbo",
    table="navigation_containers",
    schema_version_prefix="v2",
    column_list=[
        Column("navigation_container_id", "INT", uniqueness=True),
        Column("name", "VARCHAR(255)"),
        Column("handle", "VARCHAR(255)"),
        Column("store_group_id", "INT"),
        Column("archived", "BOOLEAN"),
    ],
)
