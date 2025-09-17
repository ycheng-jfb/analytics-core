from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="endowment",
    column_list=[
        Column("endowment_id", "INT", uniqueness=True),
        Column("endowment_type_id", "INT"),
        Column("endowment_batch_id", "INT"),
        Column("customer_id", "INT"),
        Column("store_credit_id", "INT"),
        Column("statuscode", "INT"),
    ],
)
