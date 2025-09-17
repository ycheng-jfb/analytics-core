from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="gift_certificate_store_credit",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("gift_certificate_id", "INT", uniqueness=True),
        Column("store_credit_id", "INT", uniqueness=True),
        Column("gift_certificate_store_credit_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
