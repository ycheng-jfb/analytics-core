from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="membership_billing",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("membership_billing_id", "INT", uniqueness=True),
        Column("membership_id", "INT"),
        Column("membership_plan_id", "INT"),
        Column("membership_type_id", "INT"),
        Column("membership_billing_source_id", "INT"),
        Column("membership_trial_id", "INT"),
        Column("activating_order_id", "INT"),
        Column("order_id", "INT"),
        Column("period_id", "INT"),
        Column("period_type", "VARCHAR(25)"),
        Column("error_message", "VARCHAR(255)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("datetime_activated", "TIMESTAMP_NTZ(3)"),
        Column("date_due", "TIMESTAMP_NTZ(0)"),
        Column("date_period_start", "TIMESTAMP_NTZ(0)"),
        Column("date_period_end", "TIMESTAMP_NTZ(0)"),
        Column("statuscode", "INT"),
    ],
)
