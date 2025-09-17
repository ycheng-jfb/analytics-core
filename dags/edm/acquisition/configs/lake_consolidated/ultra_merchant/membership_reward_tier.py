from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="membership_reward_tier",
    company_join_sql="""
        SELECT DISTINCT
            L.MEMBERSHIP_REWARD_TIER_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.MEMBERSHIP_PLAN AS MP
        ON DS.STORE_ID = MP.STORE_ID
        INNER JOIN {database}.{schema}.MEMBERSHIP_REWARD_PLAN AS MRP
        ON MP.MEMBERSHIP_PLAN_ID = MRP.MEMBERSHIP_PLAN_ID
        INNER JOIN {database}.{source_schema}.membership_reward_tier AS L
        ON L.MEMBERSHIP_REWARD_PLAN_ID = MRP.MEMBERSHIP_REWARD_PLAN_ID """,
    column_list=[
        Column("membership_reward_tier_id", "INT", uniqueness=True, key=True),
        Column("membership_reward_plan_id", "INT", key=True),
        Column("membership_level_group_id", "INT"),
        Column("label", "VARCHAR(50)"),
        Column("required_points_earned", "INT"),
        Column("required_months", "INT"),
        Column("purchase_point_multiplier", "INT"),
        Column("tier_hidden", "BOOLEAN"),
        Column("minimum_lifetime_spend", "NUMBER(19,4)"),
    ],
)
