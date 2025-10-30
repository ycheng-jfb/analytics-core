from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='membership_plan_membership_type',
    company_join_sql="""
        SELECT DISTINCT
            L.MEMBERSHIP_PLAN_MEMBERSHIP_TYPE_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.MEMBERSHIP AS M
            ON DS.STORE_ID = M.STORE_ID
        INNER JOIN {database}.{source_schema}.membership_plan_membership_type AS L
            ON L.MEMBERSHIP_PLAN_ID = M.MEMBERSHIP_PLAN_ID """,
    column_list=[
        Column('membership_plan_membership_type_id', 'INT', uniqueness=True, key=True),
        Column('membership_plan_id', 'INT', key=True),
        Column('membership_type_id', 'INT'),
        Column('membership_product_id', 'INT', key=True),
        Column('price', 'NUMBER(19, 4)'),
        Column('default_membership_reward_plan_id', 'INT', key=True),
        Column('default_shipping_option_id', 'INT'),
        Column('max_prepaid_credits', 'INT'),
        Column('membership_postreg_type_id', 'INT'),
        Column('default_membership_invitation_incentive_id', 'INT'),
        Column('default_personality_type_tag_id', 'INT'),
        Column('default_billing_day', 'INT'),
        Column('default_trial_period_days', 'INT'),
        Column('active', 'INT'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
    ],
    watermark_column='datetime_modified',
)
