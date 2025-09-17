from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="merlin",
    schema="dbo",
    table="division",
    schema_version_prefix="v2",
    column_list=[
        Column("oid", "INT", uniqueness=True, source_name="OID"),
        Column("name", "VARCHAR(100)", source_name="Name"),
        Column("inter_id", "VARCHAR(15)", source_name="InterId"),
        Column("from_addr_email", "VARCHAR(100)", source_name="FromAddrEmail"),
        Column("optimistic_lock_field", "INT", source_name="OptimisticLockField"),
        Column("gc_record", "INT", source_name="GCRecord"),
    ],
)
