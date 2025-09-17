from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="membership_trial",
    company_join_sql="""
        SELECT DISTINCT
            L.MEMBERSHIP_TRIAL_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.MEMBERSHIP AS M
            ON DS.STORE_ID = M.STORE_ID
        INNER JOIN {database}.{source_schema}.membership_trial AS L
            ON L.MEMBERSHIP_ID = M.MEMBERSHIP_ID """,
    column_list=[
        Column("membership_trial_id", "INT", uniqueness=True, key=True),
        Column("membership_id", "INT", key=True),
        Column("order_id", "INT", key=True),
        Column("trial_period_days", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("date_period_started", "TIMESTAMP_NTZ(0)"),
        Column("date_expires", "TIMESTAMP_NTZ(0)"),
        Column("datetime_cancelled", "TIMESTAMP_NTZ(3)"),
        Column("original_trial_period_days", "INT"),
        Column("original_date_expires", "TIMESTAMP_NTZ(0)"),
        Column("statuscode", "INT"),
    ],
    watermark_column="datetime_modified",
)
