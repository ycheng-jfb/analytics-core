from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="billing_retry_schedule_queue_log",
    watermark_column="billing_retry_schedule_queue_log_id",
    initial_load_value="550654135",
    enable_archive=False,
    schema_version_prefix="v2",
    column_list=[
        Column(
            "billing_retry_schedule_queue_log_id",
            "INT",
            uniqueness=True,
            delta_column=True,
        ),
        Column("billing_retry_schedule_queue_id", "INT"),
        Column("cycle", "INT"),
        Column("payment_method", "VARCHAR(25)"),
        Column("auth_payment_transaction_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)"),
    ],
)
