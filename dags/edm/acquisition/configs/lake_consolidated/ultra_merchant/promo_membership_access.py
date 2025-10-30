from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='promo_membership_access',
    company_join_sql="""
    SELECT DISTINCT
        L.PROMO_MEMBERSHIP_ACCESS_ID,
        DS.company_id
    FROM {database}.REFERENCE.DIM_STORE AS DS
    INNER JOIN {database}.{schema}.MEMBERSHIP AS MA
    ON MA.STORE_ID=DS.STORE_ID
    INNER JOIN {database}.{source_schema}.promo_membership_access AS L
        ON L.MEMBERSHIP_ID = MA.MEMBERSHIP_ID """,
    column_list=[
        Column('promo_membership_access_id', 'INT', uniqueness=True, key=True),
        Column('membership_id', 'INT', key=True),
        Column('promo_id', 'INT', key=True),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
    ],
    watermark_column='datetime_added',
)
