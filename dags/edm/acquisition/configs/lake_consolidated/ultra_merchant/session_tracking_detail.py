from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="session_tracking_detail",
    company_join_sql="""
        SELECT DISTINCT
            L.SESSION_TRACKING_DETAIL_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.SESSION AS S
            ON DS.STORE_ID = S.STORE_ID
        INNER JOIN {database}.{source_schema}.session_tracking_detail AS L
            ON L.SESSION_ID = S.SESSION_ID """,
    column_list=[
        Column("session_tracking_detail_id", "INT", uniqueness=True, key=True),
        Column("session_tracking_id", "INT", key=True),
        Column("session_id", "INT", key=True),
        Column("session_tracking_value", "VARCHAR(100)"),
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
