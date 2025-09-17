from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="merlin",
    schema="dbo",
    table="outfitstyles",
    schema_version_prefix="v2",
    column_list=[
        Column("oid", "INT", uniqueness=True, source_name="OID"),
        Column("outfit", "INT", source_name="Outfit"),
        Column("style", "INT", source_name="Style"),
        Column("style_color", "INT", source_name="StyleColor"),
        Column("optimistic_lock_field", "INT", source_name="OptimisticLockField"),
        Column("gc_record", "INT", source_name="GCRecord"),
    ],
)
