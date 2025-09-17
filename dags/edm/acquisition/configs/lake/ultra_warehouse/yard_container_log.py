from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="yard_container_log",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("yard_container_log_id", "INT", uniqueness=True),
        Column("parent_yard_container_log_id", "INT"),
        Column("warehouse_id", "INT"),
        Column("type_code_id", "INT"),
        Column("yard_container_id", "INT"),
        Column("po_number", "VARCHAR(255)"),
        Column("location_id", "INT"),
        Column("rank", "INT"),
        Column("priority_code_id", "INT"),
        Column("administrator_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
