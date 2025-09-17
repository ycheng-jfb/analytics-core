from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="inventory",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("inventory_id", "INT", uniqueness=True),
        Column("inventory_location_id", "INT"),
        Column("case_id", "INT"),
        Column("lpn_id", "INT"),
        Column("item_id", "INT"),
        Column("qty_onhand", "INT"),
        Column("qty_allocated", "INT"),
        Column("qty_incoming", "INT"),
        Column("qty_hold", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
