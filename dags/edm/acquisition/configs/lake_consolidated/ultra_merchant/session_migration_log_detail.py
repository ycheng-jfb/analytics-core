from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="session_migration_log_detail",
    company_join_sql="""
        SELECT DISTINCT
            L.SESSION_MIGRATION_LOG_DETAIL_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.SESSION AS S
            ON DS.STORE_ID = S.STORE_ID
        INNER JOIN {database}.{schema}.SESSION_MIGRATION_LOG AS SML
            ON SML.NEW_SESSION_ID = S.SESSION_ID
        INNER JOIN {database}.{source_schema}.session_migration_log_detail AS L
            ON L.SESSION_MIGRATION_LOG_ID = SML.SESSION_MIGRATION_LOG_ID """,
    column_list=[
        Column("session_migration_log_detail_id", "INT", uniqueness=True, key=True),
        Column("session_migration_log_id", "INT", key=True),
        Column("table_name", "VARCHAR(100)"),
        Column("column_name", "VARCHAR(100)"),
        Column("column_value", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_added",
)
