from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultracms",
    schema="dbo",
    table="asset_images",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("asset_images_id", "INT", uniqueness=True),
        Column("assets_id", "INT"),
        Column("image_filename", "VARCHAR(250)"),
        Column("image_metadata_json", "VARCHAR", source_name="image_metadata_JSON"),
        Column("image_misc_json", "VARCHAR", source_name="image_misc_JSON"),
        Column("display_order", "INT"),
        Column("cdn_image_filename", "VARCHAR(250)"),
        Column("statuscode", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("asset_image_type_id", "INT"),
    ],
)
