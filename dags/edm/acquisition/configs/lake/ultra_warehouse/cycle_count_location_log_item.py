from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="cycle_count_location_log_item",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("cycle_count_location_log_item_id", "INT", uniqueness=True),
        Column("cycle_count_location_log_id", "INT"),
        Column("lpn_id", "INT"),
        Column("item_id", "INT"),
        Column("quantity", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
