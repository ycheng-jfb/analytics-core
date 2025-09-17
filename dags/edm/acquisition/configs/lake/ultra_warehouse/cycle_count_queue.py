from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="cycle_count_queue",
    watermark_column="datetime_modified",
    column_list=[
        Column("cycle_count_queue_id", "INT", uniqueness=True),
        Column("warehouse_id", "INT"),
        Column("cycle_count_id", "INT"),
        Column("sequence", "INT"),
        Column("administrator_id", "INT"),
        Column("lpn_code", "VARCHAR(255)"),
        Column("status_code_id", "INT"),
        Column("location_id", "INT"),
        Column("location_label", "VARCHAR(255)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("item_id", "INT"),
        Column("cycle_count_sequence_id", "INT"),
        Column("cycle_count_directed_id", "INT"),
        Column("scan_count", "INT"),
    ],
)
