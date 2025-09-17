from include.airflow.operators.mssql_acquisition import HighWatermarkMaxRowVersion
from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="um_replicated",
    schema="dbo",
    table="ultramerchant__product_bundle_component__del",
    watermark_column="repl_timestamp",
    initial_load_value="0x0",
    high_watermark_cls=HighWatermarkMaxRowVersion,
    schema_version_prefix="v2",
    column_list=[
        Column("product_bundle_component_id", "INT"),
        Column("bundle_product_id", "INT"),
        Column("component_product_id", "INT"),
        Column("price_contribution_percentage", "DOUBLE"),
        Column("is_free", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("repl_action", "VARCHAR(1)"),
        Column("repl_timestamp", "BINARY(8)", uniqueness=True),
    ],
)
