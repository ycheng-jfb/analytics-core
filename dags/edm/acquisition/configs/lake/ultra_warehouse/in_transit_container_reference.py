from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="in_transit_container_reference",
    schema_version_prefix="v2",
    column_list=[
        Column("in_transit_container_reference_id", "INT", uniqueness=True),
        Column("to_warehouse_id", "INT"),
        Column("parent_in_transit_container_id", "INT"),
        Column("child_in_transit_container_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
