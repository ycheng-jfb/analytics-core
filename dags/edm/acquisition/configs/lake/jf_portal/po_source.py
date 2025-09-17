from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="jf_portal",
    schema="dbo",
    table="PoSource",
    watermark_column="DateUpdate",
    schema_version_prefix="v3",
    column_list=[
        Column("po_source_id", "INT", uniqueness=True, source_name="PoSourceId"),
        Column("source", "VARCHAR(50)", source_name="Source"),
        Column(
            "date_create", "TIMESTAMP_NTZ(3)", delta_column=1, source_name="DateCreate"
        ),
        Column(
            "date_update", "TIMESTAMP_NTZ(3)", delta_column=0, source_name="DateUpdate"
        ),
    ],
)
