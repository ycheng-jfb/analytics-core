from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="merlin",
    schema="dbo",
    table="StyleSizeScaleDtl",
    schema_version_prefix="v2",
    column_list=[
        Column("oid", "INT", uniqueness=True, source_name="OID"),
        Column("size_name", "VARCHAR(50)", source_name="SizeName"),
        Column('"ORDER"', "INT", source_name="[Order]"),
        Column("sku_segment", "VARCHAR(5)", source_name="SkuSegment"),
        Column("style_size_scale_hdr", "INT", source_name="StyleSizeScaleHdr"),
        Column("optimistic_lock_field", "INT", source_name="OptimisticLockField"),
        Column("gc_record", "INT", source_name="GCRecord"),
        Column("nrf_size_code_std", "VARCHAR(5)", source_name="NRFSizeCodeStd"),
    ],
)
