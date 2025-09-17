from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="membership_billing_cohort_billing_date",
    company_join_sql="""
        SELECT DISTINCT
            L.MEMBERSHIP_BILLING_COHORT_BILLING_DATE_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.MEMBERSHIP_PLAN AS MP
            ON DS.STORE_ID = MP.STORE_ID
        INNER JOIN {database}.{schema}.MEMBERSHIP_BILLING_COHORT AS MBC
            ON MP.MEMBERSHIP_PLAN_ID = MBC.MEMBERSHIP_PLAN_ID
        INNER JOIN {database}.{source_schema}.membership_billing_cohort_billing_date AS L
            ON MBC.MEMBERSHIP_BILLING_COHORT_ID = L.MEMBERSHIP_BILLING_COHORT_ID """,
    column_list=[
        Column(
            "membership_billing_cohort_billing_date_id",
            "INT",
            uniqueness=True,
            key=True,
        ),
        Column("membership_billing_cohort_id", "INT", key=True),
        Column("percentage", "DOUBLE"),
        Column("date_due", "TIMESTAMP_NTZ(0)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)"),
        Column("administrator_id", "INT"),
    ],
    watermark_column="datetime_added",
)
