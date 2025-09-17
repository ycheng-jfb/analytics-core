from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="membership_credit_billing_stats_log",
    watermark_column="datetime_report",
    column_list=[
        Column("membership_plan_id", "INT", uniqueness=True),
        Column("datetime_report", "TIMESTAMP_NTZ(0)", uniqueness=True, delta_column=0),
        Column("date_report", "TIMESTAMP_NTZ(0)"),
        Column("label", "VARCHAR(50)", uniqueness=True),
        Column("last_hour_attempted", "INT"),
        Column("last_hour_count", "INT"),
        Column("total_credited", "INT"),
        Column("total_credited_today", "INT"),
        Column("total_marked_for_credit", "INT"),
        Column("total_pending_past_due_date", "INT"),
        Column("total_future_pending", "INT"),
        Column("total_members_at_start", "INT"),
        Column("total_members_in_retry", "INT"),
        Column("total_retries_today", "INT"),
        Column("total_retry_successes_today", "INT"),
        Column("total_retry_successes", "INT"),
    ],
)
