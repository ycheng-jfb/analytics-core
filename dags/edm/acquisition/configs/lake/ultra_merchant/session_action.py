from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="session_action",
    schema_version_prefix="v2",
    column_list=[
        Column("session_action_id", "INT", uniqueness=True),
        Column("label", "VARCHAR(50)"),
    ],
)
