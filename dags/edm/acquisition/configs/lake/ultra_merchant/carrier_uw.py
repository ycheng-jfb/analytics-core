from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="carrier_uw",
    watermark_column="datetime_modified",
    strict_inequality=True,
    schema_version_prefix="v2",
    column_list=[
        Column("carrier_uw_id", "INT", uniqueness=True),
        Column("carrier_id", "INT"),
        Column("label", "VARCHAR(255)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("tracking_url", "VARCHAR(255)"),
        Column("carrier_hash", "BINARY(20)"),
    ],
)
