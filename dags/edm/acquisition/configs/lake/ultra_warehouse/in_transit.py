from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="in_transit",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("in_transit_id", "INT", uniqueness=True),
        Column("label", "VARCHAR(255)"),
        Column("po_number", "VARCHAR(255)"),
        Column("carrier_id", "INT"),
        Column("from_warehouse_id", "INT"),
        Column("to_warehouse_id", "INT"),
        Column("adjustment_id", "INT"),
        Column("datetime_cancelled", "TIMESTAMP_NTZ(3)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("status_code_id", "INT"),
        Column("fulfillment_id", "INT"),
        Column("shipment_volume", "DOUBLE"),
        Column("shipment_carton_count", "INT"),
        Column("bill_of_lading", "VARCHAR(255)"),
        Column("vessel_name", "VARCHAR(255)"),
        Column("voyage_number", "VARCHAR(255)"),
        Column("pod_warehouse_id", "INT"),
        Column("transport_mode", "VARCHAR(255)"),
        Column("datetime_transaction", "TIMESTAMP_NTZ(3)"),
        Column("datetime_local_transaction", "TIMESTAMP_NTZ(3)"),
        Column("type_code_id", "INT"),
    ],
)
