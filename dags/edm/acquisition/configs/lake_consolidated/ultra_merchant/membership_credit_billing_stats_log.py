from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='membership_credit_billing_stats_log',
    company_join_sql="""
        SELECT DISTINCT
            L.MEMBERSHIP_PLAN_ID,
            L.DATETIME_REPORT,
            L.LABEL,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.MEMBERSHIP AS M
            ON DS.STORE_ID = M.STORE_ID
        INNER JOIN {database}.{source_schema}.membership_credit_billing_stats_log AS L
            ON L.MEMBERSHIP_PLAN_ID = M.MEMBERSHIP_PLAN_ID """,
    column_list=[
        Column('membership_plan_id', 'INT', uniqueness=True, key=True),
        Column(
            'datetime_report',
            'TIMESTAMP_NTZ(0)',
            uniqueness=True,
        ),
        Column('date_report', 'TIMESTAMP_NTZ(0)'),
        Column('label', 'VARCHAR(50)', uniqueness=True),
        Column('last_hour_attempted', 'INT'),
        Column('last_hour_count', 'INT'),
        Column('total_credited', 'INT'),
        Column('total_credited_today', 'INT'),
        Column('total_marked_for_credit', 'INT'),
        Column('total_pending_past_due_date', 'INT'),
        Column('total_future_pending', 'INT'),
        Column('total_members_at_start', 'INT'),
        Column('total_members_in_retry', 'INT'),
        Column('total_retries_today', 'INT'),
        Column('total_retry_successes_today', 'INT'),
        Column('total_retry_successes', 'INT'),
    ],
    watermark_column='datetime_report',
)
