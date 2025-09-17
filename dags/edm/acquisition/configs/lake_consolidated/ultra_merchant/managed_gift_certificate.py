from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="managed_gift_certificate",
    company_join_sql="""
         SELECT DISTINCT
          L.gift_certificate_id,
          DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{source_schema}.gift_certificate AS G
         ON DS.STORE_GROUP_ID = G.STORE_GROUP_ID
        INNER JOIN {database}.{source_schema}.managed_gift_certificate AS L
         ON G.GIFT_CERTIFICATE_ID = L.GIFT_CERTIFICATE_ID """,
    column_list=[
        Column("gift_certificate_id", "INT", uniqueness=True, key=True),
        Column("gift_certificate_provider_id", "INT", key=True),
        Column("code", "VARCHAR(25)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)"),
    ],
    watermark_column="datetime_added",
)
