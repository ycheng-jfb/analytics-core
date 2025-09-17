from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="store_warehouse_map",
    schema_version_prefix="v2",
    column_list=[
        Column("store_id", "INT", uniqueness=True),
        Column("country_code", "VARCHAR(2)", uniqueness=True),
        Column("warehouse_id", "INT"),
        Column("shipper_item_number", "VARCHAR(30)"),
    ],
)
