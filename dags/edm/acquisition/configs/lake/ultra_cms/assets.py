from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultracms",
    schema="dbo",
    table="assets",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("assets_id", "INT", uniqueness=True),
        Column("store_group_id", "INT"),
        Column("asset_container_id", "INT"),
        Column("label", "VARCHAR(200)"),
        Column("description", "VARCHAR(500)"),
        Column("statuscode", "INT"),
        Column("start_datetime", "TIMESTAMP_NTZ(3)"),
        Column("end_datetime", "TIMESTAMP_NTZ(3)"),
        Column("segmentation_id", "INT"),
        Column("assets_json", "VARCHAR", source_name="assets_JSON"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("priority", "INT"),
        Column("assetprioritycheck", "INT"),
        Column("administrator_id", "INT"),
        Column("ongoing_asset", "INT"),
    ],
)
