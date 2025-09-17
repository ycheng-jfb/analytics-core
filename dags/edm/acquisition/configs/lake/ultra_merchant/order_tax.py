from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="order_tax",
    watermark_column="datetime_modified",
    column_list=[
        Column("order_tax_id", "INT", uniqueness=True),
        Column("order_id", "INT"),
        Column("type", "VARCHAR(25)"),
        Column("tax_rate", "DOUBLE"),
        Column("amount", "NUMBER(19, 4)"),
        Column("taxable_amount", "NUMBER(19, 4)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
