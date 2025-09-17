from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="lpn",
    watermark_column="datetime_modified",
    initial_load_value="2020-01-14",
    schema_version_prefix="v2",
    column_list=[
        Column("lpn_id", "INT", uniqueness=True),
        Column("lpn_code", "VARCHAR(255)"),
        Column("company_id", "INT"),
        Column("warehouse_id", "INT"),
        Column("item_id", "INT"),
        Column("receipt_id", "INT"),
        Column("lot_id", "INT"),
        Column("fulfillment_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("status_code_id", "INT"),
        Column("las_po_id", "INT"),
        Column("in_transit_container_id", "INT"),
        Column("print_count", "INT"),
    ],
)
