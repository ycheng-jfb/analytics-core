from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="receipt_item",
    watermark_column="datetime_modified",
    schema_version_prefix="v3",
    column_list=[
        Column("receipt_item_id", "INT", uniqueness=True),
        Column("receipt_id", "INT"),
        Column("lot_id", "INT"),
        Column("item_id", "INT"),
        Column("quantity", "INT"),
        Column("quantity_cancelled", "INT"),
        Column("administrator_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("foreign_po_line_number", "VARCHAR(30)"),
        Column("erp_notified", "BOOLEAN"),
    ],
)
