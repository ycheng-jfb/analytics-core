from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="product_category",
    watermark_column="datetime_modified",
    strict_inequality=True,
    schema_version_prefix="v2",
    column_list=[
        Column("product_category_id", "INT", uniqueness=True),
        Column("store_group_id", "INT"),
        Column("parent_product_category_id", "INT"),
        Column("label", "VARCHAR(50)"),
        Column("short_description", "VARCHAR(255)"),
        Column("medium_description", "VARCHAR(2000)"),
        Column("long_description", "VARCHAR"),
        Column("full_image_content_id", "INT"),
        Column("thumbnail_image_content_id", "INT"),
        Column("active", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("product_category_type", "VARCHAR(50)"),
        Column("archive", "BOOLEAN"),
    ],
)
