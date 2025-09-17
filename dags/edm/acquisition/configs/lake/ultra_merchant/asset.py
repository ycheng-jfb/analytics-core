from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="asset",
    schema_version_prefix="v3",
    watermark_column="datetime_modified",
    column_list=[
        Column("asset_id", "INT", uniqueness=True),
        Column("object", "VARCHAR(50)"),
        Column("object_id", "INT"),
        Column("asset_type_id", "INT"),
        Column("asset_category_id", "INT"),
        Column("filename", "VARCHAR(255)"),
        Column("path", "VARCHAR(255)"),
        Column("width", "INT"),
        Column("height", "INT"),
        Column("sort", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("statuscode", "INT"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("is_main_image", "BOOLEAN", source_name="isMainImage"),
        Column("is_hover_image", "BOOLEAN", source_name="isHoverImage"),
    ],
)
