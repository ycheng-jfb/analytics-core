from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="membership_billing_cohort",
    enable_archive=False,
    schema_version_prefix="v2",
    column_list=[
        Column("membership_billing_cohort_id", "INT", uniqueness=True),
        Column("membership_plan_id", "INT"),
        Column("period_id", "INT"),
        Column("date_activated_start", "TIMESTAMP_NTZ(0)"),
        Column("date_activated_end", "TIMESTAMP_NTZ(0)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
