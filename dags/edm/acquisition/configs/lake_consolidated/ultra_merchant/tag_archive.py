from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="tag_archive",
    company_join_sql="""
        SELECT DISTINCT
            L.TAG_ARCHIVE_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{source_schema}.tag_archive AS L
            ON DS.STORE_GROUP_ID = L.MASTER_STORE_GROUP_ID """,
    column_list=[
        Column("tag_archive_id", "INT", uniqueness=True, key=True),
        Column("tag_id", "INT", key=True),
        Column("master_store_group_id", "INT"),
        Column("active", "INT"),
        Column("administrator_id_added", "INT"),
        Column("tag_archive_datetime_added", "TIMESTAMP_NTZ(3)"),
        Column("administrator_id_deleted", "INT"),
        Column("tag_archive_datetime_deleted", "TIMESTAMP_NTZ(3)"),
    ],
)
