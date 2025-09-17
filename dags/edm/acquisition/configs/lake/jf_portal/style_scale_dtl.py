from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="JF_Portal",
    schema="dbo",
    table="StyleScaleDTL",
    watermark_column="DateUpdate",
    schema_version_prefix="v2",
    column_list=[
        Column(
            "style_scale_dtl_id", "INT", uniqueness=True, source_name="StyleScaleDtlId"
        ),
        Column("scale", "VARCHAR(50)", source_name="Scale"),
        Column("name", "VARCHAR(50)", source_name="Name"),
        Column('"ORDER"', "INT", source_name="[Order]"),
        Column("sku_segment", "VARCHAR(5)", source_name="SKUSegment"),
        Column("user_create", "VARCHAR(100)", source_name="UserCreate"),
        Column("user_update", "VARCHAR(100)", source_name="UserUpdate"),
        Column(
            "date_create", "TIMESTAMP_NTZ(3)", delta_column=1, source_name="DateCreate"
        ),
        Column(
            "date_update", "TIMESTAMP_NTZ(3)", delta_column=0, source_name="DateUpdate"
        ),
        Column("mv2_oid", "INT", source_name="MV2_Oid"),
    ],
)
