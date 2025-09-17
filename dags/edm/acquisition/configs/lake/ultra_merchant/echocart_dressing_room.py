from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="echocart_dressing_room",
    watermark_column="datetime_modified",
    column_list=[
        Column("echocart_dressing_room_id", "INT", uniqueness=True),
        Column("echocart_id", "INT"),
        Column("store_id", "INT"),
        Column("dressing_room_id", "INT"),
        Column("datetime_exited", "TIMESTAMP_NTZ(3)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
