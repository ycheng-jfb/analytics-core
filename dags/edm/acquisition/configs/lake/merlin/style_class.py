from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="merlin",
    schema="dbo",
    table="StyleClass",
    schema_version_prefix="v2",
    column_list=[
        Column("oid", "INT", uniqueness=True, source_name="OID"),
        Column("class", "VARCHAR(30)", source_name="Class"),
        Column("optimistic_lock_field", "INT", source_name="OptimisticLockField"),
        Column("gc_record", "INT", source_name="GCRecord"),
        Column("sc_category", "INT", source_name="SCCategory"),
        Column("master_class", "INT", source_name="MasterClass"),
        Column("case_pack", "INT", source_name="CasePack"),
        Column("round_to_case", "BOOLEAN", source_name="RoundToCase"),
        Column("us", "INT", source_name="US"),
        Column("ca", "INT", source_name="CA"),
        Column("msrp_mapping_hdr", "INT", source_name="MsrpMappingHdr"),
    ],
)
