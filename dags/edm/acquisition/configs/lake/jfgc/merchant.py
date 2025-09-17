from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="jfgc",
    schema="dbo",
    table="merchant",
    schema_version_prefix="v2",
    column_list=[
        Column("merchant_id", "INT", uniqueness=True),
        Column("provider_id", "INT"),
        Column("label", "VARCHAR(100)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("statuscode", "INT"),
    ],
)
