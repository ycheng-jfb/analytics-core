from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="order_line_warehouse_mapping_log_detail",
    watermark_column="datetime_added",
    schema_version_prefix="v2",
    column_list=[
        Column("order_line_warehouse_mapping_log_detail_id", "INT", uniqueness=True),
        Column("order_line_warehouse_mapping_log_id", "INT"),
        Column("order_id", "INT"),
        Column("order_line_id", "INT"),
        Column("item_id", "INT"),
        Column("quantity", "INT"),
        Column("selected_warehouse_id", "INT"),
        Column("available_warehouse_id", "INT"),
        Column("available_quantity", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("preallocated_quantity", "INT"),
    ],
)
