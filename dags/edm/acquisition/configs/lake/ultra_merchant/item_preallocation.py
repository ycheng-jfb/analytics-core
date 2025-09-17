from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="item_preallocation",
    schema_version_prefix="v2",
    column_list=[
        Column("item_id", "INT", uniqueness=True),
        Column("warehouse_id", "INT", uniqueness=True),
        Column("quantity", "INT"),
    ],
)
