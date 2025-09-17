from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="membership_settings",
    company_join_sql="""
        SELECT DISTINCT
            L.MEMBERSHIP_SETTINGS_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{source_schema}.membership_settings AS L
            ON DS.STORE_ID = L.STORE_ID """,
    column_list=[
        Column("membership_settings_id", "INT", uniqueness=True, key=True),
        Column("store_id", "INT"),
        Column("store_domain_type_id", "INT"),
        Column("dm_site_id", "INT", key=True),
        Column("membership_plan_id", "INT", key=True),
        Column("membership_signup_type_id", "INT"),
        Column("quiz_id", "INT", key=True),
        Column("weight", "INT"),
        Column("invitation_only", "INT"),
        Column("dm_traffic_default", "INT"),
        Column("session_detail", "VARCHAR(4000)"),
        Column("membership_type_id", "INT"),
        Column("membership_brand_id", "INT"),
    ],
)
