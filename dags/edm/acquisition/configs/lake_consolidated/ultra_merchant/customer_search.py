from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="customer_search",
    company_join_sql="""
     SELECT DISTINCT
         L.customer_search_id,
         DS.COMPANY_ID
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{source_schema}.customer_search AS L
     ON DS.STORE_GROUP_ID = L.STORE_GROUP_ID """,
    column_list=[
        Column("customer_search_id", "INT", uniqueness=True, key=True),
        Column("customer_id", "INT", key=True),
        Column("session_id", "INT", key=True),
        Column("store_group_id", "INT"),
        Column("query_text", "VARCHAR(500)"),
        Column("query_time_ms", "INT"),
        Column("query_count", "INT"),
        Column("query_version", "VARCHAR(25)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("keywords", "VARCHAR(500)"),
        Column("facet", "INT"),
        Column("autosuggest", "INT"),
    ],
    watermark_column="datetime_modified",
)
