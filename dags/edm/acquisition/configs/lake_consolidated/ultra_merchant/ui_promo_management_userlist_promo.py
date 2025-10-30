from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='ui_promo_management_userlist_promo',
    company_join_sql="""
        SELECT DISTINCT
            L.UI_PROMO_MANAGEMENT_USERLIST_PROMO_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.PROMO AS P
            ON DS.STORE_GROUP_ID = P.STORE_GROUP_ID
        INNER JOIN {database}.{source_schema}.ui_promo_management_userlist_promo AS L
            ON P.PROMO_ID = L.PROMO_ID """,
    column_list=[
        Column('ui_promo_management_userlist_promo_id', 'INT', uniqueness=True, key=True),
        Column('ui_promo_management_userlist_id', 'INT'),
        Column('promo_id', 'INT', key=True),
        Column('membership_promo_type_id', 'INT'),
        Column('membership_table', 'VARCHAR(50)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('promo_expiration_days', 'INT'),
    ],
    watermark_column='datetime_modified',
)
