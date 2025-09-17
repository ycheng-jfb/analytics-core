from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="adjustment",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("adjustment_id", "INT", uniqueness=True),
        Column("warehouse_id", "INT"),
        Column("label", "VARCHAR(255)"),
        Column("description", "VARCHAR(1024)"),
        Column("putaway_location_id", "INT"),
        Column("reason_code_id", "INT"),
        Column("reason_code_other", "VARCHAR(255)"),
        Column("administrator_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("status_code_id", "INT"),
        Column("foreign_order_id", "INT"),
        Column("wholesaler_id", "INT", source_name="WholesalerId"),
        Column("fulfillment_replen_batch_id", "INT"),
        Column("type_code_id", "INT"),
    ],
)
