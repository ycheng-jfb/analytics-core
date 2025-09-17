from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="membership_billing_credit_threshold",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("membership_billing_credit_threshold_id", "INT", uniqueness=True),
        Column("membership_plan_id", "INT"),
        Column("period_id", "INT"),
        Column("max_credits", "INT"),
        Column("range_mins", "INT"),
        Column("date_billing", "TIMESTAMP_NTZ(0)"),
        Column("datetime_start", "TIMESTAMP_NTZ(3)"),
        Column("datetime_end", "TIMESTAMP_NTZ(3)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
