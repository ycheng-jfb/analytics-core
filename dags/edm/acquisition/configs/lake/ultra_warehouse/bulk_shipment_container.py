from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="bulk_shipment_container",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("bulk_shipment_container_id", "INT", uniqueness=True),
        Column("bulk_shipment_id", "INT"),
        Column("label", "VARCHAR(255)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("status_code_id", "INT"),
    ],
)
