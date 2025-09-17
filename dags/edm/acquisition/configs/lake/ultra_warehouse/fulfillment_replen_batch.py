from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="fulfillment_replen_batch",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("fulfillment_replen_batch_id", "INT", uniqueness=True),
        Column("warehouse_id", "INT"),
        Column("administrator_id", "INT"),
        Column("datetime_batch", "TIMESTAMP_NTZ(3)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("type_code_id", "INT"),
        Column("label", "VARCHAR(255)"),
        Column("outbound_plan_id", "INT"),
    ],
)
