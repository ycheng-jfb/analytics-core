from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="product_property",
    watermark_column="datetime_added",
    schema_version_prefix="v2",
    column_list=[
        Column("product_property_id", "INT", uniqueness=True),
        Column("product_id", "INT"),
        Column("product_property_type_id", "INT"),
        Column("label", "VARCHAR(50)"),
        Column("value", "VARCHAR(255)"),
        Column("sort", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
