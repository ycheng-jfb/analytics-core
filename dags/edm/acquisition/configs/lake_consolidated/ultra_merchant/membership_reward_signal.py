from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="membership_reward_signal",
    company_join_sql="""
        SELECT DISTINCT
            L.MEMBERSHIP_REWARD_SIGNAL_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.MEMBERSHIP AS C
            ON DS.STORE_ID = C.STORE_ID
        INNER JOIN {database}.{source_schema}.membership_reward_signal AS L
            ON L.MEMBERSHIP_ID = C.MEMBERSHIP_ID """,
    column_list=[
        Column("membership_reward_signal_id", "INT", uniqueness=True, key=True),
        Column("membership_id", "INT", key=True),
        Column("membership_reward_tier_id", "INT", key=True),
        Column("membership_level_id", "INT"),
        Column("membership_tier_points", "INT"),
        Column("points_to_next_tier", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_modified",
)
