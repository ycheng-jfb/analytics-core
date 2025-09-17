from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="language",
    schema_version_prefix="v2",
    column_list=[
        Column("language_id", "INT", uniqueness=True),
        Column("code", "VARCHAR(5)"),
        Column("label", "VARCHAR(50)"),
    ],
)
