from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="merlin",
    schema="dbo",
    table="Color",
    schema_version_prefix="v2",
    column_list=[
        Column("oid", "INT", uniqueness=True, source_name="OID"),
        Column("name", "VARCHAR(50)", source_name="Name"),
        Column("nrf_code", "VARCHAR(5)", source_name="NrfCode"),
        Column("nrf_color_tone_name", "VARCHAR(50)", source_name="NrfColorToneName"),
        Column("pantone", "VARCHAR(30)", source_name="Pantone"),
        Column("web_color_name", "VARCHAR(50)", source_name="WebColorName"),
        Column("img_file_path", "VARCHAR(500)", source_name="ImgFilePath"),
        Column("color_family", "INT", source_name="ColorFamily"),
        Column("optimistic_lock_field", "INT", source_name="OptimisticLockField"),
        Column("gc_record", "INT", source_name="GCRecord"),
        Column("description", "VARCHAR(100)", source_name="Description"),
        Column("usage", "INT", source_name="Usage"),
        Column("nrf_code_std", "VARCHAR(3)", source_name="NRFCodeStd"),
    ],
)
