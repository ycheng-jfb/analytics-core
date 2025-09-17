from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="membership_suggestion_product_purchase",
    watermark_column="datetime_added",
    enable_archive=False,
    schema_version_prefix="v2",
    column_list=[
        Column("membership_suggestion_product_purchase_id", "INT", uniqueness=True),
        Column("membership_suggestion_product_id", "INT"),
        Column("order_id", "INT"),
        Column("product_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
