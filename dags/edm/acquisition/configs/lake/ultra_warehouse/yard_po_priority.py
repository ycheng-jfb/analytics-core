from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="yard_po_priority",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("yard_po_priority_id", "INT", uniqueness=True),
        Column("po_number", "VARCHAR(255)"),
        Column("priority_code_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
