from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="url_rewrite",
    company_join_sql="""
        SELECT DISTINCT
            L.URL_REWRITE_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{source_schema}.url_rewrite AS L
            ON DS.STORE_GROUP_ID = L.STORE_GROUP_ID """,
    column_list=[
        Column("url_rewrite_id", "INT", uniqueness=True, key=True),
        Column("store_group_id", "INT"),
        Column("path", "VARCHAR(255)"),
        Column("fuseaction", "VARCHAR(50)"),
        Column("page_key", "VARCHAR(50)"),
        Column("target", "VARCHAR(255)"),
        Column("attribute_list", "VARCHAR(255)"),
        Column("title", "VARCHAR(100)"),
        Column("keywords", "VARCHAR(500)"),
        Column("description", "VARCHAR(200)"),
        Column("notes", "VARCHAR(500)"),
        Column("active", "INT"),
        Column("is_global", "INT"),
    ],
)
