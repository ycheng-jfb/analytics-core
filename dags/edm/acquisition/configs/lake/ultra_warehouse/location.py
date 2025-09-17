from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="location",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("location_id", "INT", uniqueness=True),
        Column("location_code", "VARCHAR(16)"),
        Column("label", "VARCHAR(255)"),
        Column("description", "VARCHAR(512)"),
        Column("warehouse_id", "INT"),
        Column("zone_id", "INT"),
        Column("pick_zone_id", "INT"),
        Column("allow_multiple_item", "INT"),
        Column("data_1", "VARCHAR(50)"),
        Column("data_2", "VARCHAR(50)"),
        Column("data_3", "VARCHAR(50)"),
        Column("data_4", "VARCHAR(50)"),
        Column("data_meld", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("action_status_code_id", "INT"),
        Column("status_code_id", "INT"),
    ],
)
