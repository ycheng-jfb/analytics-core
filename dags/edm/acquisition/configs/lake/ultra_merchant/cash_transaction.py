from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="cash_transaction",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("cash_transaction_id", "INT", uniqueness=True),
        Column("amount", "NUMBER(19, 4)"),
        Column("balance", "NUMBER(19, 4)"),
        Column("object", "VARCHAR(50)"),
        Column("object_id", "INT"),
        Column("parent_cash_transaction_id", "INT"),
        Column("administrator_id", "INT"),
        Column("cash_drawer_id", "INT"),
        Column("statuscode", "INT"),
        Column("memo", "VARCHAR(255)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
