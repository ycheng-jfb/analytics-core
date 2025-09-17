from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="membership",
    company_join_sql="""
     SELECT DISTINCT
         L.membership_id,
         DS.company_id
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{schema}.membership_plan AS mp
      ON DS.STORE_ID= mp.STORE_ID
     INNER JOIN {database}.{source_schema}.membership AS L
     ON L.MEMBERSHIP_PLAN_ID=mp.MEMBERSHIP_PLAN_ID""",
    column_list=[
        Column("membership_id", "INT", uniqueness=True, key=True),
        Column("customer_id", "INT", key=True),
        Column("store_id", "INT"),
        Column("membership_type_id", "INT"),
        Column("membership_plan_id", "INT", key=True),
        Column("membership_completion_method_id", "INT"),
        Column("membership_signup_id", "INT", key=True),
        Column("membership_reward_plan_id", "INT", key=True),
        Column("order_id", "INT", key=True),
        Column("order_tracking_id", "INT"),
        Column("discount_id", "INT", key=True),
        Column("shipping_option_id", "INT"),
        Column("offer_id", "INT"),
        Column("shipping_address_id", "INT", key=True),
        Column("payment_method", "VARCHAR(25)"),
        Column("payment_object_id", "INT"),
        Column("payment_option_id", "INT"),
        Column("current_membership_recommendation_id", "INT"),
        Column("current_period_id", "INT"),
        Column("next_period_id", "INT"),
        Column("membership_level_id", "INT"),
        Column("membership_team_id", "INT"),
        Column("price", "NUMBER(19, 4)"),
        Column("max_prepaid_credits", "INT"),
        Column("date_added", "TIMESTAMP_NTZ(0)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("date_next_scheduled", "TIMESTAMP_NTZ(0)"),
        Column("datetime_activated", "TIMESTAMP_NTZ(3)"),
        Column("datetime_cancelled", "TIMESTAMP_NTZ(3)"),
        Column("date_expires", "TIMESTAMP_NTZ(0)"),
        Column("statuscode", "INT"),
        Column("membership_reward_tier_id", "INT", key=True),
        Column("date_reward_tier_recalculate", "TIMESTAMP_NTZ(0)"),
        Column("date_reward_tier_updated", "TIMESTAMP_NTZ(0)"),
        Column("billing_month", "INT"),
        Column("billing_day", "INT"),
        Column("membership_brand_id", "INT"),
    ],
    watermark_column="datetime_modified",
)
