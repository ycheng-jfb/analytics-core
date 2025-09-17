from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="chargeback_container_target",
    watermark_column="datetime_modified",
    strict_inequality=True,
    schema_version_prefix="v2",
    column_list=[
        Column("chargeback_container_target_id", "INT", uniqueness=True),
        Column("container_type", "VARCHAR(255)"),
        Column("usage_target", "INT"),
        Column("usage_min", "INT"),
        Column("traffic_mode", "VARCHAR(255)"),
        Column("is_active", "BOOLEAN"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
