from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ssrs_reports",
    schema="gsc",
    table="pol_pod_tranist_logic",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("traffic_mode", "VARCHAR(255)"),
        Column("pol", "VARCHAR(255)"),
        Column("pol_warehouse_id", "INT"),
        Column("pod", "VARCHAR(255)"),
        Column("pod_warehouse_id", "INT"),
        Column("destination", "VARCHAR(255)"),
        Column("total_transit_time", "INT"),
        Column("destination_warehouse_id", "INT"),
        Column("transit_logic", "VARCHAR(255)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("pol_pod_tranist_logic_id", "INT", uniqueness=True),
    ],
)
