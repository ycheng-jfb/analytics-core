from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ssrs_reports",
    schema="dbo",
    table="POAuditTrail",
    schema_version_prefix="v2",
    column_list=[
        Column("doc_id", "INT", source_name="DocId"),
        Column("division", "VARCHAR(15)", source_name="Division"),
        Column("showroom", "VARCHAR(20)", source_name="Showroom"),
        Column("po_number", "VARCHAR(17)", source_name="PONumber"),
        Column("vendor", "VARCHAR(255)", source_name="Vendor"),
        Column("class", "VARCHAR(255)", source_name="Class"),
        Column("po_status", "VARCHAR(255)", source_name="POStatus"),
        Column("department", "VARCHAR(255)", source_name="Department"),
        Column("total_quantity", "INT", source_name="TotalQuantity"),
        Column("tables", "VARCHAR", source_name="Tables"),
        Column("column_name", "VARCHAR", source_name="ColumnName"),
        Column("user_name", "VARCHAR", source_name="UserName"),
        Column("line", "VARCHAR", source_name="Line"),
        Column("old_value", "VARCHAR", source_name="OldValue"),
        Column("new_value", "VARCHAR", source_name="NewValue"),
        Column("change_date", "TIMESTAMP_NTZ(3)", source_name="ChangeDate"),
        Column("change_reason", "VARCHAR", source_name="ChangeReason"),
    ],
)
