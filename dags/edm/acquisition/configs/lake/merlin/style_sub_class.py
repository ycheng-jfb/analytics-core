from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="merlin",
    schema="dbo",
    table="StyleSubClass",
    schema_version_prefix="v2",
    column_list=[
        Column("oid", "INT", uniqueness=True, source_name="OID"),
        Column("subclass", "VARCHAR(40)", source_name="Subclass"),
        Column("class", "INT", source_name="Class"),
        Column("optimistic_lock_field", "INT", source_name="OptimisticLockField"),
        Column("gc_record", "INT", source_name="GCRecord"),
    ],
)
