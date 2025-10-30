from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='membership_plan',
    company_join_sql="""
      SELECT DISTINCT
          L.membership_plan_id,
          DS.company_id
      FROM {database}.REFERENCE.DIM_STORE AS DS
      INNER JOIN {database}.{source_schema}.membership_plan AS L
       ON DS.STORE_GROUP_ID = L.STORE_GROUP_ID """,
    column_list=[
        Column('membership_plan_id', 'INT', uniqueness=True, key=True),
        Column('store_group_id', 'INT'),
        Column('store_id', 'INT'),
        Column('membership_type_id', 'INT'),
        Column('product_category_id', 'INT', key=True),
        Column('membership_product_id', 'INT', key=True),
        Column('default_membership_reward_plan_id', 'INT', key=True),
        Column('default_membership_recommendation_method_id', 'INT'),
        Column('default_shipping_option_id', 'INT'),
        Column('default_membership_team_id', 'INT'),
        Column('period_type', 'VARCHAR(25)'),
        Column('label', 'VARCHAR(50)'),
        Column('description', 'VARCHAR(2000)'),
        Column('price', 'NUMBER(19, 4)'),
        Column('max_prepaid_credits', 'INT'),
        Column('recommendation_request_extension_days', 'INT'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('active', 'INT'),
        Column('membership_postreg_type_id', 'INT'),
        Column('default_membership_invitation_incentive_id', 'INT'),
        Column('default_personality_type_tag_id', 'INT'),
        Column('default_billing_day', 'INT'),
        Column('default_trial_period_days', 'INT'),
        Column('allow_credit_for_fee', 'INT'),
        Column('default_membership_brand_id', 'INT'),
    ],
    watermark_column='datetime_added',
)
