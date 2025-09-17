from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="las_po_item",
    watermark_column="datetime_modified",
    schema_version_prefix="v3",
    column_list=[
        Column("las_po_item_id", "INT", uniqueness=True),
        Column("las_po_id", "INT"),
        Column("item_id", "INT"),
        Column("item_number", "VARCHAR(255)"),
        Column("quantity_ordered", "INT"),
        Column("quantity_cancelled", "INT"),
        Column("quantity_sku_printed", "INT"),
        Column("quantity_lpn_assigned", "INT"),
        Column("quantity_lpn_packed", "INT"),
        Column("quantity_lpn_intransit", "INT"),
        Column("quantity_case_packed", "INT"),
        Column("quantity_case_intransit", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("quantity_case_printed", "INT"),
        Column("quantity_floorset", "INT"),
        Column("label_size", "VARCHAR(1)"),
        Column("quantity_lpn_per_case", "INT"),
        Column("quantity_lpn_pad", "INT"),
        Column("quantity_case_pad", "INT"),
        Column("receiving_warehouse_id", "INT"),
        Column("foreign_po_line_number", "VARCHAR(30)"),
    ],
)
