from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="promo_classification",
    schema_version_prefix="v3",
    column_list=[
        Column("promo_classification_id", "INT", uniqueness=True),
        Column("label", "VARCHAR(50)"),
        Column("parent_promo_classification_id", "INT"),
        Column("sort", "INT"),
    ],
)
