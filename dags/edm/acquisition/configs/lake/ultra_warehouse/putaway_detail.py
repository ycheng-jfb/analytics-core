from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="putaway_detail",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("putaway_detail_id", "INT", uniqueness=True),
        Column("putaway_id", "INT"),
        Column("parent_container_id", "INT"),
        Column("container_id", "INT"),
        Column("case_id", "INT"),
        Column("case_qty", "INT"),
        Column("lpn_id", "INT"),
        Column("item_id", "INT"),
        Column("location_id", "INT"),
        Column("quantity", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("cycle_count_location_id", "INT"),
        Column("did_cycle_count_recover_unit", "INT"),
        Column("cancel_type_code_id", "INT"),
        Column("status_code_id", "INT"),
        Column("remaining_qty", "INT"),
    ],
)
