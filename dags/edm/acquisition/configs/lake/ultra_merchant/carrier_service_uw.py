from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="carrier_service_uw",
    watermark_column="datetime_modified",
    schema_version_prefix="v3",
    column_list=[
        Column("carrier_service_uw_id", "INT", uniqueness=True),
        Column("carrier_service_id", "INT"),
        Column("carrier_id", "INT"),
        Column("code", "VARCHAR(25)"),
        Column("label", "VARCHAR(255)"),
        Column("rate_version", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("carrier_service_hash", "BINARY(20)"),
        Column("external_code", "VARCHAR(25)"),
    ],
)
