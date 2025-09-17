from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="in_transit_container_case_detail",
    watermark_column="datetime_modified",
    schema_version_prefix="v3",
    column_list=[
        Column("in_transit_container_case_detail_id", "INT", uniqueness=True),
        Column("in_transit_container_case_id", "INT"),
        Column("lpn_id", "INT"),
        Column("item_id", "INT"),
        Column("quantity", "INT"),
        Column("quantity_received", "INT"),
        Column("quantity_cancelled", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("quantity_added", "INT"),
        Column("carton_count", "INT"),
        Column("volume", "DOUBLE"),
        Column("weight", "DOUBLE"),
        Column("foreign_po_line_number", "VARCHAR(30)"),
    ],
)
