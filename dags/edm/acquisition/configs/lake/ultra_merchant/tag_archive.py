from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="tag_archive",
    schema_version_prefix="v2",
    column_list=[
        Column("tag_archive_id", "INT", uniqueness=True),
        Column("tag_id", "INT"),
        Column("master_store_group_id", "INT"),
        Column("active", "INT"),
        Column("administrator_id_added", "INT"),
        Column("tag_archive_datetime_added", "TIMESTAMP_NTZ(3)"),
        Column("administrator_id_deleted", "INT"),
        Column("tag_archive_datetime_deleted", "TIMESTAMP_NTZ(3)"),
    ],
)
