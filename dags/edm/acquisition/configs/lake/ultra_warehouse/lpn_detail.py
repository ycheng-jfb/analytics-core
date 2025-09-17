from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="lpn_detail",
    watermark_column="datetime_modified",
    column_list=[
        Column("lpn_detail_id", "INT", uniqueness=True),
        Column("lpn_id", "INT"),
        Column("name", "VARCHAR(35)"),
        Column("value", "VARCHAR(500)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
