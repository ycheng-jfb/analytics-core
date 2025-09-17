from include.airflow.operators.mssql_acquisition import HighWatermarkMaxRowVersion
from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="um_replicated",
    schema="dbo",
    table="ultramerchant__order_line_discount__del",
    watermark_column="repl_timestamp",
    initial_load_value="0x0",
    high_watermark_cls=HighWatermarkMaxRowVersion,
    schema_version_prefix="v2",
    column_list=[
        Column("order_line_discount_id", "INT"),
        Column("order_id", "INT"),
        Column("order_line_id", "INT"),
        Column("promo_id", "INT"),
        Column("discount_id", "INT"),
        Column("amount", "NUMBER(19, 4)"),
        Column("indirect_discount", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("repl_action", "VARCHAR(1)"),
        Column("repl_timestamp", "BINARY(8)", uniqueness=True),
    ],
)
