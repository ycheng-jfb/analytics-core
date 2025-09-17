from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="Merlin",
    schema="dbo",
    table="State",
    watermark_column="oid",
    initial_load_value="0",
    strict_inequality=True,
    enable_archive=False,
    schema_version_prefix="v2",
    column_list=[
        Column("oid", "INT", uniqueness=True, delta_column=True, source_name="OID"),
        Column("name", "VARCHAR(255)", source_name="Name"),
        Column("abbreviation", "VARCHAR(10)", source_name="Abbreviation"),
        Column("type", "VARCHAR(255)", source_name="Type"),
        Column("assoc_press", "VARCHAR(255)", source_name="AssocPress"),
        Column(
            "standard_federal_region",
            "VARCHAR(255)",
            source_name="StandardFederalRegion",
        ),
        Column("census_region", "VARCHAR(255)", source_name="CensusRegion"),
        Column("census_region_name", "VARCHAR(255)", source_name="CensusRegionName"),
        Column("census_division", "VARCHAR(255)", source_name="CensusDivision"),
        Column(
            "census_division_name", "VARCHAR(255)", source_name="CensusDivisionName"
        ),
        Column("circuit_court", "VARCHAR(255)", source_name="CircuitCourt"),
        Column("country", "VARCHAR(2)", source_name="Country"),
        Column("optimistic_lock_field", "INT", source_name="OptimisticLockField"),
        Column("gc_record", "INT", source_name="GCRecord"),
    ],
)
