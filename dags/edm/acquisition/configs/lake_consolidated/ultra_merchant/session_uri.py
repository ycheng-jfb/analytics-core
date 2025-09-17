from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="session_uri",
    company_join_sql="""
        SELECT DISTINCT
            L.SESSION_URI_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.SESSION AS S
            ON DS.STORE_ID = S.STORE_ID
        INNER JOIN {database}.{source_schema}.session_uri AS L
            ON L.SESSION_ID = S.SESSION_ID """,
    column_list=[
        Column("session_uri_id", "INT", uniqueness=True, key=True),
        Column("session_uri_hash_id", "INT"),
        Column("session_id", "INT", key=True),
        Column("referer", "VARCHAR"),
        Column("uri", "VARCHAR(4096)"),
        Column("user_agent", "VARCHAR(512)"),
        Column("device_type_id", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_modified",
)
