from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="store_credit_conversion",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("store_credit_conversion_id", "INT", uniqueness=True),
        Column("original_store_credit_id", "INT"),
        Column("converted_store_credit_id", "INT"),
        Column("store_credit_conversion_type_id", "INT"),
        Column("administrator_id", "INT"),
        Column("comment", "VARCHAR(255)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
