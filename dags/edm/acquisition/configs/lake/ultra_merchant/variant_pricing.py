from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="variant_pricing",
    watermark_column="datetime_modified",
    strict_inequality=True,
    schema_version_prefix="v2",
    column_list=[
        Column("variant_pricing_id", "INT", uniqueness=True),
        Column("test_metadata_id", "INT"),
        Column("product_id", "INT"),
        Column("pricing_id", "INT"),
        Column("control_pricing_id", "INT"),
        Column("variant_number", "INT"),
        Column("statuscode", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
