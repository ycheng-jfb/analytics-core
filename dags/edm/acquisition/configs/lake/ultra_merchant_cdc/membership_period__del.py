from include.airflow.operators.mssql_acquisition import HighWatermarkMaxRowVersion
from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="um_replicated",
    schema="dbo",
    table="ultramerchant__membership_period__del",
    watermark_column="repl_timestamp",
    initial_load_value="0x0",
    high_watermark_cls=HighWatermarkMaxRowVersion,
    schema_version_prefix="v2",
    column_list=[
        Column("membership_period_id", "INT"),
        Column("membership_id", "INT"),
        Column("period_id", "INT"),
        Column("credit_order_id", "INT"),
        Column("error_message", "VARCHAR(255)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("date_due", "TIMESTAMP_NTZ(0)"),
        Column("statuscode", "INT"),
        Column("membership_plan_id", "INT"),
        Column("priority", "INT"),
        Column("datetime_activated", "TIMESTAMP_NTZ(3)"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("date_start", "TIMESTAMP_NTZ(0)"),
        Column("date_end", "TIMESTAMP_NTZ(0)"),
        Column("repl_action", "VARCHAR(1)"),
        Column("repl_timestamp", "BINARY(8)", uniqueness=True),
    ],
)
