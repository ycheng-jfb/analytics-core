from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="store_classification",
    schema_version_prefix="v2",
    column_list=[
        Column("store_classification_id", "INT", uniqueness=True),
        Column("store_id", "INT"),
        Column("store_type_id", "INT"),
    ],
)
