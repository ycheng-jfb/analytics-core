from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='gift_certificate_store_credit',
    company_join_sql="""
        SELECT DISTINCT
            L.GIFT_CERTIFICATE_ID,
            L.STORE_CREDIT_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.GIFT_CERTIFICATE AS GC
            ON DS.STORE_GROUP_ID = GC.STORE_GROUP_ID
        INNER JOIN {database}.{source_schema}.gift_certificate_store_credit AS L
            ON L.GIFT_CERTIFICATE_ID=GC.GIFT_CERTIFICATE_ID""",
    column_list=[
        Column('gift_certificate_id', 'INT', uniqueness=True, key=True),
        Column('store_credit_id', 'INT', uniqueness=True, key=True),
        Column('gift_certificate_store_credit_id', 'INT', key=True),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
    ],
    watermark_column='datetime_modified',
)
