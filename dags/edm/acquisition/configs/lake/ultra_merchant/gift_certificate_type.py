from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    watermark_column="datetime_added",
    table="gift_certificate_type",
    schema_version_prefix="v2",
    column_list=[
        Column("gift_certificate_type_id", "INT", uniqueness=True),
        Column("label", "VARCHAR(50)"),
        Column("description", "VARCHAR(255)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
