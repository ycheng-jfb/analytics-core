from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="order_surcharge",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("order_surcharge_id", "INT", uniqueness=True),
        Column("order_id", "INT"),
        Column("type", "VARCHAR(25)"),
        Column("rate", "DOUBLE"),
        Column("subtotal_before_surcharge", "NUMBER(19, 4)"),
        Column("surchargeable_amount", "NUMBER(19, 4)"),
        Column("surcharge_amount_raw", "NUMBER(19, 4)"),
        Column("surcharge_amount_rounded", "NUMBER(19, 4)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
