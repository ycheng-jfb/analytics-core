from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='membership_billing_cohort',
    company_join_sql="""
        SELECT DISTINCT
            L.MEMBERSHIP_BILLING_COHORT_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.MEMBERSHIP_PLAN AS MP
            ON DS.STORE_ID = MP.STORE_ID
        INNER JOIN {database}.{source_schema}.membership_billing_cohort AS L
            ON MP.MEMBERSHIP_PLAN_ID = L.MEMBERSHIP_PLAN_ID """,
    column_list=[
        Column('membership_billing_cohort_id', 'INT', uniqueness=True, key=True),
        Column('membership_plan_id', 'INT', key=True),
        Column('period_id', 'INT'),
        Column('date_activated_start', 'TIMESTAMP_NTZ(0)'),
        Column('date_activated_end', 'TIMESTAMP_NTZ(0)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
    ],
)
