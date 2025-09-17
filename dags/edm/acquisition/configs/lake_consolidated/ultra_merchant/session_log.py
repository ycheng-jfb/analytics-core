from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="session_log",
    company_join_sql="""
        SELECT DISTINCT
            L.SESSION_LOG_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.SESSION AS S
            ON DS.STORE_ID = S.STORE_ID
        INNER JOIN {database}.{source_schema}.session_log AS L
            ON L.SESSION_ID = S.SESSION_ID """,
    partition_cols=["session_log_id", "session_log_hash_id"],
    column_list=[
        Column("session_log_id", "INT", uniqueness=True, key=True),
        Column("session_log_hash_id", "INT"),
        Column("session_id", "INT", key=True),
        Column("session_action_id", "INT"),
        Column("object", "VARCHAR(50)"),
        Column("object_id", "INT"),
        Column("action_count", "INT"),
        Column("store_domain_id", "INT", key=True),
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
