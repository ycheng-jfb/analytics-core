from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="cycle_count_sequence_inventory",
    watermark_column="datetime_modified",
    column_list=[
        Column("cycle_count_sequence_inventory_id", "INT", uniqueness=True),
        Column("cycle_count_id", "INT"),
        Column("cycle_count_sequence_id", "INT"),
        Column("inventory_id", "INT"),
        Column("inventory_location_id", "INT"),
        Column("location_id", "INT"),
        Column("lpn_id", "INT"),
        Column("item_id", "INT"),
        Column("cycle_count_queue_id", "INT"),
        Column("inventory_log_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("move_to_lost", "BOOLEAN"),
        Column("sequence_1_scanned", "BOOLEAN"),
        Column("sequence_2_scanned", "INT"),
    ],
)
