from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="Merlin",
    schema="dbo",
    table="FxRateCurrency",
    watermark_column="DateUpdate",
    schema_version_prefix="v2",
    column_list=[
        Column("oid", "INT", uniqueness=True, source_name="OID"),
        Column("source_currency", "INT", source_name="SourceCurrency"),
        Column("dest_currency", "INT", source_name="DestCurrency"),
        Column("exgtblid", "VARCHAR(20)", source_name="EXGTBLID"),
        Column("active", "BOOLEAN", source_name="Active"),
        Column("optimistic_lock_field", "INT", source_name="OptimisticLockField"),
        Column("gc_record", "INT", source_name="GCRecord"),
        Column("exgtblid_avg", "VARCHAR(20)", source_name="EXGTBLID_AVG"),
        Column(
            "date_create", "TIMESTAMP_NTZ(3)", delta_column=1, source_name="DateCreate"
        ),
        Column(
            "date_update", "TIMESTAMP_NTZ(3)", delta_column=0, source_name="DateUpdate"
        ),
    ],
)
