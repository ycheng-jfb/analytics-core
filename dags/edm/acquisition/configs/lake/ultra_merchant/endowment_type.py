from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="endowment_type",
    column_list=[
        Column("endowment_type_id", "INT", uniqueness=True),
        Column("label", "VARCHAR(50)"),
    ],
)
