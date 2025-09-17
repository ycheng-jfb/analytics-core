from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="order_shipment",
    schema_version_prefix="v3",
    watermark_column="datetime_modified",
    column_list=[
        Column("order_shipment_id", "INT", uniqueness=True),
        Column("order_id", "INT"),
        Column("warehouse_id", "INT"),
        Column("shipping_address_id", "INT"),
        Column("date_shipped", "TIMESTAMP_NTZ(0)"),
        Column("carrier_id", "INT"),
        Column("carrier_service_id", "INT"),
        Column("zone", "INT"),
        Column("rate", "NUMBER(19, 4)"),
        Column("rate_weight", "DOUBLE"),
        Column("tracking_number", "VARCHAR(50)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("statuscode", "INT"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("delivered", "INT"),
        Column("date_delivered", "TIMESTAMP_NTZ(0)"),
        Column("date_estimate_delivery", "TIMESTAMP_NTZ(3)"),
    ],
)
