from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="merlin",
    schema="dbo",
    table="ColorFamily",
    schema_version_prefix="v2",
    column_list=[
        Column("oid", "INT", uniqueness=True, source_name="OID"),
        Column("name", "VARCHAR(50)", source_name="Name"),
        Column("nrf_color", "VARCHAR(50)", source_name="NrfColor"),
        Column("optimistic_lock_field", "INT", source_name="OptimisticLockField"),
        Column("gc_record", "INT", source_name="GCRecord"),
    ],
)
