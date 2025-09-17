from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="ui_promo_management_userlist_users",
    company_join_sql="""
        SELECT DISTINCT
            L.UI_PROMO_MANAGEMENT_USERS_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.MEMBERSHIP AS M
            ON DS.STORE_ID = M.STORE_ID
        INNER JOIN {database}.{source_schema}.ui_promo_management_userlist_users AS L
            ON M.MEMBERSHIP_ID = L.MEMBERSHIP_ID """,
    column_list=[
        Column("ui_promo_management_users_id", "INT", uniqueness=True, key=True),
        Column("ui_promo_management_userlist_id", "INT"),
        Column("membership_id", "INT", key=True),
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
