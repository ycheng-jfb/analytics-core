from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="bulk_shipment_container_detail",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("bulk_shipment_container_detail_id", "INT", uniqueness=True),
        Column("bulk_shipment_container_id", "INT"),
        Column("parent_container_id", "INT"),
        Column("container_id", "INT"),
        Column("case_id", "INT"),
        Column("lpn_id", "INT"),
        Column("item_id", "INT"),
        Column("quantity", "INT"),
        Column("inventory_log_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
