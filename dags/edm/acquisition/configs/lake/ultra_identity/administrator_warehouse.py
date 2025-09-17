from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultraidentity",
    schema="dbo",
    table="administrator_warehouse",
    watermark_column="datetime_added",
    enable_archive=True,
    schema_version_prefix="v2",
    column_list=[
        Column("administrator_id", "INT", uniqueness=True),
        Column("warehouse_id", "INT", uniqueness=True),
        Column("granter_administrator_id", "INT"),
        Column("badge_id", "VARCHAR(50)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
