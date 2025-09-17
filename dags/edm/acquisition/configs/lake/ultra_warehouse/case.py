from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="[case]",
    watermark_column="datetime_modified",
    schema_version_prefix="v3",
    column_list=[
        Column("case_id", "INT", uniqueness=True),
        Column("case_code", "VARCHAR(255)"),
        Column("company_id", "INT"),
        Column("box_id", "INT"),
        Column("receipt_id", "INT"),
        Column("lot_id", "INT"),
        Column("weight", "DOUBLE"),
        Column("warehouse_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("status_code_id", "INT"),
        Column("in_transit_container_id", "INT"),
        Column("las_po_id", "INT"),
        Column("print_count", "INT"),
        Column("cube_id", "INT"),
        Column("case_item_cube_id", "INT"),
        Column("las_po_item_id", "INT"),
    ],
)
