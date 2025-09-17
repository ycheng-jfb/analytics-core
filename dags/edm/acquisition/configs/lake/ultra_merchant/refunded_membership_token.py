from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="refunded_membership_token",
    watermark_column="datetime_added",
    schema_version_prefix="v2",
    column_list=[
        Column("refunded_membership_token_id", "INT", uniqueness=True),
        Column("refund_id", "INT"),
        Column("membership_token_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
