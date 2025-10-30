from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='membership_reward_plan',
    company_join_sql="""
        SELECT DISTINCT
            L.MEMBERSHIP_REWARD_PLAN_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.MEMBERSHIP AS M
            ON DS.STORE_ID = M.STORE_ID
        INNER JOIN {database}.{source_schema}.membership_reward_plan AS L
            ON L.MEMBERSHIP_PLAN_ID = M.MEMBERSHIP_PLAN_ID """,
    column_list=[
        Column('membership_reward_plan_id', 'INT', uniqueness=True, key=True),
        Column('membership_plan_id', 'INT', key=True),
        Column('label', 'VARCHAR(50)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('purchase_point_multiplier', 'DOUBLE'),
        Column('default_reward_credit_redemption_type', 'VARCHAR(50)'),
        Column('eligible_membership_level_group_ids', 'VARCHAR(50)'),
        Column('membership_cancellation_forfeits_points', 'BOOLEAN'),
        Column('tier_calculation_months', 'INT'),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
    ],
    watermark_column='datetime_modified',
)
