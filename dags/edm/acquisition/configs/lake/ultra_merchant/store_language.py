from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="store_language",
    schema_version_prefix="v2",
    column_list=[
        Column("store_language_id", "INT", uniqueness=True),
        Column("store_id", "INT"),
        Column("language_id", "INT"),
    ],
)
