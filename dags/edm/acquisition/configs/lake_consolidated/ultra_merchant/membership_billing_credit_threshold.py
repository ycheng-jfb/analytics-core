from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="membership_billing_credit_threshold",
    company_join_sql="""
        SELECT DISTINCT
            L.MEMBERSHIP_BILLING_CREDIT_THRESHOLD_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.MEMBERSHIP_PLAN AS MP
            ON DS.STORE_ID = MP.STORE_ID
        INNER JOIN {database}.{source_schema}.membership_billing_credit_threshold AS L
            ON MP.MEMBERSHIP_PLAN_ID = L.MEMBERSHIP_PLAN_ID """,
    column_list=[
        Column(
            "membership_billing_credit_threshold_id", "INT", uniqueness=True, key=True
        ),
        Column("membership_plan_id", "INT", key=True),
        Column("period_id", "INT"),
        Column("max_credits", "INT"),
        Column("range_mins", "INT"),
        Column("date_billing", "TIMESTAMP_NTZ(0)"),
        Column("datetime_start", "TIMESTAMP_NTZ(3)"),
        Column("datetime_end", "TIMESTAMP_NTZ(3)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("administrator_id", "INT"),
    ],
    watermark_column="datetime_modified",
)
