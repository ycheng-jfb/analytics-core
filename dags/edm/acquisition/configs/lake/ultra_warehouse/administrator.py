from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="administrator",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("administrator_id", "INT", uniqueness=True),
        Column("login", "VARCHAR(25)"),
        Column("firstname", "VARCHAR(25)"),
        Column("lastname", "VARCHAR(25)"),
        Column("active", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
