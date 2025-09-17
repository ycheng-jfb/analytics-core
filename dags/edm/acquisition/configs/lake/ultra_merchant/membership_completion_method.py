from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="membership_completion_method",
    schema_version_prefix="v2",
    column_list=[
        Column("membership_completion_method_id", "INT", uniqueness=True),
        Column("label", "VARCHAR(50)"),
    ],
)
