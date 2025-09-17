from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="featured_product_location",
    watermark_column="datetime_modified",
    schema_version_prefix="v3",
    column_list=[
        Column("featured_product_location_id", "INT", uniqueness=True),
        Column("store_group_id", "INT"),
        Column("label", "VARCHAR(100)"),
        Column("max_products", "INT"),
        Column("exclude_products_from_membership_recommendation", "INT"),
        Column("featured_product_location_type_id", "INT"),
        Column("active", "INT"),
        Column("page_title", "VARCHAR(50)"),
        Column("page_url", "VARCHAR(128)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("code", "VARCHAR(50)"),
        Column("membership_brand_id_list", "VARCHAR(50)"),
    ],
)
