from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="inventory_log_container_item",
    watermark_column="inventory_log_container_item_id",
    initial_load_value="4072150179",
    strict_inequality=True,
    schema_version_prefix="v2",
    column_list=[
        Column("inventory_log_container_item_id", "INT", uniqueness=True),
        Column("inventory_log_id", "INT"),
        Column("lpn_id", "INT"),
        Column("item_id", "INT"),
        Column("quantity", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
