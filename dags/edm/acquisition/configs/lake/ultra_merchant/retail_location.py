from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="retail_location",
    schema_version_prefix="v2",
    column_list=[
        Column("retail_location_id", "INT", uniqueness=True),
        Column("retailer_id", "INT"),
        Column("label", "VARCHAR(100)"),
        Column("address", "VARCHAR(100)"),
        Column("city", "VARCHAR(35)"),
        Column("state", "VARCHAR(2)"),
        Column("zip", "VARCHAR(10)"),
        Column("phone", "VARCHAR(25)"),
        Column("store_id", "INT"),
        Column("hours", "VARCHAR(200)"),
        Column("latitude", "VARCHAR(50)"),
        Column("longitude", "VARCHAR(50)"),
        Column("statuscode", "INT"),
        Column("data", "VARCHAR(1000)"),
    ],
)
