from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="in_transit_container_case",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("in_transit_container_case_id", "INT", uniqueness=True),
        Column("in_transit_container_id", "INT"),
        Column("label", "VARCHAR(255)"),
        Column("case_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("in_transit_document_id", "INT"),
        Column("package_id", "INT"),
        Column("customs_case_id", "INT"),
    ],
)
