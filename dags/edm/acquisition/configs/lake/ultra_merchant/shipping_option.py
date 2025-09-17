from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="shipping_option",
    watermark_column="datetime_added",
    schema_version_prefix="v2",
    column_list=[
        Column("shipping_option_id", "INT", uniqueness=True),
        Column("type", "VARCHAR(25)"),
        Column("label", "VARCHAR(50)"),
        Column("cost", "NUMBER(19, 4)"),
        Column("amount", "NUMBER(19, 4)"),
        Column("description", "VARCHAR(255)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=True),
    ],
)
