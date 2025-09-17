from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="merlin",
    schema="dbo",
    table="outfit",
    watermark_column="DateUpdate",
    schema_version_prefix="v2",
    column_list=[
        Column("oid", "INT", uniqueness=True, source_name="OID"),
        Column("outfit_id", "VARCHAR(100)", source_name="OutfitID"),
        Column("outfit_name", "VARCHAR(100)", source_name="OutfitName"),
        Column("user_create", "VARCHAR(36)", source_name="UserCreate"),
        Column("user_update", "VARCHAR(36)", source_name="UserUpdate"),
        Column(
            "date_create", "TIMESTAMP_NTZ(3)", delta_column=1, source_name="DateCreate"
        ),
        Column(
            "date_update", "TIMESTAMP_NTZ(3)", delta_column=0, source_name="DateUpdate"
        ),
        Column("optimistic_lock_field", "INT", source_name="OptimisticLockField"),
        Column("gc_record", "INT", source_name="GCRecord"),
        Column("is_bundle", "BOOLEAN", source_name="IsBundle"),
        Column("launch_date", "TIMESTAMP_NTZ(3)", source_name="LaunchDate"),
        Column("alias", "VARCHAR(50)", source_name="Alias"),
        Column("is_sgc", "BOOLEAN", source_name="IsSGC"),
        Column("is_plus_size", "BOOLEAN", source_name="IsPlusSize"),
        Column("active", "BOOLEAN", source_name="Active"),
        Column("skus", "VARCHAR(500)", source_name="Skus"),
    ],
)
