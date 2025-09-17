from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="pick",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("pick_id", "INT", uniqueness=True),
        Column("type_code_id", "INT"),
        Column("type_parent_id", "INT"),
        Column("parent_id", "INT"),
        Column("location_id", "INT"),
        Column("parent_container_id", "INT"),
        Column("case_id", "INT"),
        Column("lpn_id", "INT"),
        Column("item_id", "INT"),
        Column("quantity", "INT"),
        Column("pick_assigned_administrator_id", "INT"),
        Column("pick_administrator_id", "INT"),
        Column("datetime_pick_assigned", "TIMESTAMP_NTZ(3)"),
        Column("datetime_picked", "TIMESTAMP_NTZ(3)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("sort", "INT"),
        Column("quantity_type_needed", "INT"),
        Column("quantity_type_inprogress", "INT"),
        Column("case_has_lpn", "BOOLEAN"),
        Column("pick_has_rush", "BOOLEAN"),
    ],
)
