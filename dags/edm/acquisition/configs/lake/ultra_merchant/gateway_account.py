from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="gateway_account",
    column_list=[
        Column("gateway_account_id", "INT", uniqueness=True),
        Column("gateway_id", "INT"),
        Column("label", "VARCHAR(50)"),
        Column("account", "VARCHAR(150)"),
        Column("password", "VARCHAR(150)"),
        Column("merchant_id", "VARCHAR(50)"),
        Column("descriptor_override", "VARCHAR(25)"),
        Column("phone_override", "VARCHAR(25)"),
        Column("enable_creditcard_update", "INT"),
        Column("active", "INT"),
    ],
)
