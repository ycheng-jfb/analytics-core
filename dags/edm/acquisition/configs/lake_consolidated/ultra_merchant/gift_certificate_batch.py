from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='gift_certificate_batch',
    company_join_sql="""
        SELECT DISTINCT
            L.GIFT_CERTIFICATE_BATCH_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{source_schema}.gift_certificate_batch AS L
            ON DS.STORE_GROUP_ID = L.STORE_GROUP_ID """,
    column_list=[
        Column('gift_certificate_batch_id', 'INT', uniqueness=True, key=True),
        Column('store_group_id', 'INT'),
        Column('label', 'VARCHAR(50)'),
        Column('description', 'VARCHAR(255)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
    ],
    watermark_column='datetime_added',
)
