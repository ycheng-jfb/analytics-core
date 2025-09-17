from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultracart",
    schema="dbo",
    table="cart_source_type",
    schema_version_prefix="v2",
    column_list=[
        Column("cart_source_type_id", "INT", uniqueness=True),
        Column("label", "VARCHAR(50)"),
    ],
)
