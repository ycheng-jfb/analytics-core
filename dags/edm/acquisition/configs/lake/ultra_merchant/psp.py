from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="psp",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("psp_id", "INT", uniqueness=True),
        Column("customer_id", "INT"),
        Column("address_id", "INT"),
        Column("token", "VARCHAR(100)"),
        Column("payment_method", "VARCHAR(50)"),
        Column("type", "VARCHAR(50)"),
        Column("acct_num", "VARCHAR(50)"),
        Column("acct_name", "VARCHAR(50)"),
        Column("exp_month", "VARCHAR(2)"),
        Column("exp_year", "VARCHAR(2)"),
        Column("bank_name", "VARCHAR(50)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("statuscode", "INT"),
        Column("alias", "VARCHAR"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("contract", "VARCHAR(20)"),
    ],
)
