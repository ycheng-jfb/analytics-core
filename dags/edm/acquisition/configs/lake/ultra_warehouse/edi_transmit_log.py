from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="edi_transmit_log",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("warehouse_id", "INT", uniqueness=True),
        Column("object", "VARCHAR(255)", uniqueness=True),
        Column("object_id", "VARCHAR(255)", uniqueness=True),
        Column("datetime_sent", "TIMESTAMP_NTZ(3)"),
        Column("datetime_received", "TIMESTAMP_NTZ(3)"),
        Column("response", "VARCHAR(8000)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
