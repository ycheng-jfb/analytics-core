from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="item_reference",
    watermark_column="datetime_modified",
    strict_inequality=True,
    schema_version_prefix="v2",
    column_list=[
        Column("item_reference_id", "INT", uniqueness=True),
        Column("wholesaler_id", "INT", source_name="WholesalerId"),
        Column("item_id", "INT"),
        Column("type_code_id", "INT"),
        Column("label", "VARCHAR(255)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
