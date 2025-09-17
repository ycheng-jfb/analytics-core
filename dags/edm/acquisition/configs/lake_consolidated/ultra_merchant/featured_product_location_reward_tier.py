from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="featured_product_location_reward_tier",
    company_join_sql="""
       SELECT DISTINCT
           L.FEATURED_PRODUCT_LOCATION_REWARD_TIER_ID,
           DS.COMPANY_ID
       FROM {database}.REFERENCE.DIM_STORE AS DS
       INNER JOIN {database}.{schema}.FEATURED_PRODUCT_LOCATION AS FPL
           ON FPL.STORE_GROUP_ID = DS.STORE_GROUP_ID
       INNER JOIN {database}.{source_schema}.featured_product_location_reward_tier AS L
           ON L.FEATURED_PRODUCT_LOCATION_ID = FPL.FEATURED_PRODUCT_LOCATION_ID """,
    column_list=[
        Column(
            "featured_product_location_reward_tier_id", "INT", uniqueness=True, key=True
        ),
        Column("featured_product_location_id", "INT", key=True),
        Column("points", "INT"),
    ],
)
