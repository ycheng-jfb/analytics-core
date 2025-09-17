from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="pricing_option",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("pricing_option_id", "INT", uniqueness=True),
        Column("pricing_id", "INT"),
        Column("break_quantity", "INT"),
        Column("unit_price", "NUMBER(19, 4)"),
        Column("shipping_price", "NUMBER(19, 4)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
