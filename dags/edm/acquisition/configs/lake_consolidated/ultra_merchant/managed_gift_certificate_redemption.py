from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='managed_gift_certificate_redemption',
    company_join_sql="""
         SELECT DISTINCT
          L.managed_gift_certificate_redemption_id,
          DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{source_schema}.gift_certificate AS G
         ON DS.STORE_GROUP_ID = G.STORE_GROUP_ID
        INNER JOIN {database}.{source_schema}.managed_gift_certificate AS M
         ON G.GIFT_CERTIFICATE_ID = M.GIFT_CERTIFICATE_ID
        INNER JOIN {database}.{source_schema}.managed_gift_certificate_redemption AS L
         ON M.GIFT_CERTIFICATE_ID = L.MANAGED_GIFT_CERTIFICATE_ID """,
    column_list=[
        Column('managed_gift_certificate_redemption_id', 'INT', uniqueness=True, key=True),
        Column('managed_gift_certificate_id', 'INT', key=True),
        Column('redemption_gift_certificate_id', 'INT', key=True),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)'),
    ],
    watermark_column='datetime_added',
)
