from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="cycle_count_location_log",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("cycle_count_location_log_id", "INT", uniqueness=True),
        Column("cycle_count_location_id", "INT"),
        Column("sequence", "INT"),
        Column("parent_container_id", "INT"),
        Column("container_id", "INT"),
        Column("case_id", "INT"),
        Column("lpn_id", "INT"),
        Column("item_id", "INT"),
        Column("quantity", "INT"),
        Column("previous_quantity", "INT"),
        Column("is_accurate", "INT"),
        Column("disposition_type_code_id", "INT"),
        Column("error_message", "VARCHAR(255)"),
        Column("inventory_log_id", "INT"),
        Column("administrator_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
