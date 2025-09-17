from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="dressing_room",
    watermark_column="datetime_modified",
    strict_inequality=True,
    schema_version_prefix="v3",
    column_list=[
        Column("dressing_room_id", "INT", uniqueness=True),
        Column("label", "VARCHAR(200)"),
        Column("store_id", "INT"),
        Column("barcode", "VARCHAR(50)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("serial_number", "VARCHAR(35)"),
        Column("battery_voltage", "INT"),
        Column("statuscode", "INT"),
    ],
)
