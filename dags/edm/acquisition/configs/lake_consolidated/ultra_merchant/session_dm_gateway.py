from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="session_dm_gateway",
    company_join_sql="""
        SELECT DISTINCT
            L.SESSION_DM_GATEWAY_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.SESSION AS S
            ON DS.STORE_ID = S.STORE_ID
        INNER JOIN {database}.{source_schema}.session_dm_gateway AS L
            ON L.SESSION_ID = S.SESSION_ID """,
    column_list=[
        Column("session_dm_gateway_id", "INT", uniqueness=True, key=True),
        Column("session_id", "INT", key=True),
        Column("dm_gateway_id", "INT", key=True),
        Column("dm_site_id", "INT", key=True),
        Column("dm_gateway_test_site_id", "INT", key=True),
        Column("order_tracking_id", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("statuscode", "INT"),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_modified",
)
