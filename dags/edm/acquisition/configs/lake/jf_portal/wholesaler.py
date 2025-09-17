from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="jf_portal",
    schema="dbo",
    table="Wholesaler",
    schema_version_prefix="v3",
    watermark_column="DateUpdate",
    column_list=[
        Column("wholesaler_id", "INT", uniqueness=True, source_name="WholesalerId"),
        Column("name", "VARCHAR(100)", source_name="Name"),
        Column("po_suffix", "VARCHAR(2)", source_name="POSuffix"),
        Column(
            "date_create", "TIMESTAMP_NTZ(3)", delta_column=1, source_name="DateCreate"
        ),
        Column(
            "date_update", "TIMESTAMP_NTZ(3)", delta_column=0, source_name="DateUpdate"
        ),
    ],
)
