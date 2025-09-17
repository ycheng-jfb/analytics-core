from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="order_vat",
    watermark_column="datetime_modified",
    column_list=[
        Column("order_vat_id", "INT", uniqueness=True),
        Column("order_id", "INT"),
        Column("vat_rate", "DOUBLE"),
        Column("amount", "NUMBER(19, 4)"),
        Column("deferred_tax", "NUMBER(19, 4)"),
        Column("item_vat", "NUMBER(19, 4)"),
        Column("shipping_vat", "NUMBER(19, 4)"),
        Column("vat_credited", "NUMBER(19, 4)"),
        Column("deferred_credit_vat_country", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("country_code", "VARCHAR(2)"),
    ],
)
