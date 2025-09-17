from include.airflow.operators.mssql_acquisition import HighWatermarkMaxRowVersion
from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="um_replicated",
    schema="dbo",
    table="ultramerchant__exchange__del",
    watermark_column="repl_timestamp",
    initial_load_value="0x0",
    high_watermark_cls=HighWatermarkMaxRowVersion,
    schema_version_prefix="v2",
    column_list=[
        Column("exchange_id", "INT"),
        Column("rma_id", "INT"),
        Column("original_order_id", "INT"),
        Column("exchange_order_id", "INT"),
        Column("session_id", "INT"),
        Column("refund_return_action_id", "INT"),
        Column("charge_shipping", "INT"),
        Column("charge_return_shipping", "INT"),
        Column("date_added", "TIMESTAMP_NTZ(0)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("statuscode", "INT"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("repl_action", "VARCHAR(1)"),
        Column("repl_timestamp", "BINARY(8)", uniqueness=True),
    ],
)
