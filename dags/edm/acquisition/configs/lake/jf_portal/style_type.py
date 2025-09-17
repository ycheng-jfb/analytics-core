from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="jf_portal",
    schema="dbo",
    table="StyleType",
    is_full_upsert=True,
    schema_version_prefix="v2",
    column_list=[
        Column("style_type_id", "INT", uniqueness=True, source_name="StyleTypeId"),
        Column("name", "VARCHAR(50)", source_name="Name"),
        Column("description", "VARCHAR(500)", source_name="Description"),
        Column("case_pack", "INT", source_name="CasePack"),
        Column("sku_segment", "VARCHAR(2)", source_name="SKUSegment"),
        Column("user_create", "VARCHAR(100)", source_name="UserCreate"),
        Column("user_update", "VARCHAR(100)", source_name="UserUpdate"),
        Column(
            "date_create", "TIMESTAMP_NTZ(3)", delta_column=1, source_name="DateCreate"
        ),
        Column(
            "date_update", "TIMESTAMP_NTZ(3)", delta_column=0, source_name="DateUpdate"
        ),
        Column("round_to_case", "BOOLEAN", source_name="RoundToCase"),
    ],
)
