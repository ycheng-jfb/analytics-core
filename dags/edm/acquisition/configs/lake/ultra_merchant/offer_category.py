from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="offer_category",
    watermark_column="datetime_added",
    schema_version_prefix="v2",
    column_list=[
        Column("offer_category_id", "INT", uniqueness=True),
        Column("store_group_id", "INT"),
        Column("parent_offer_category_id", "INT"),
        Column("label", "VARCHAR(50)"),
        Column("sort", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
