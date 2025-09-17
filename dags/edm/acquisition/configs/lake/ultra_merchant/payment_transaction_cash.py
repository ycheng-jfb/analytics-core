from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="payment_transaction_cash",
    watermark_column="datetime_modified",
    initial_load_value="2020-01-12",
    schema_version_prefix="v2",
    column_list=[
        Column("payment_transaction_id", "INT", uniqueness=True),
        Column("order_id", "INT"),
        Column("work_location_id", "VARCHAR(50)"),
        Column("transaction_type", "VARCHAR(25)"),
        Column("amount", "NUMBER(19, 4)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("statuscode", "INT"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("amount_tendered", "NUMBER(19, 4)"),
        Column("administrator_id", "INT"),
    ],
)
