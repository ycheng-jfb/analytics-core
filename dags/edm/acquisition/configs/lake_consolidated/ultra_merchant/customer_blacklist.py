from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='customer_blacklist',
    company_join_sql="""
        SELECT DISTINCT
            L.customer_blacklist_id,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.CUSTOMER AS C
            ON DS.STORE_GROUP_ID = C.STORE_GROUP_ID
        INNER JOIN {database}.{source_schema}.customer_blacklist AS L
            ON L.CUSTOMER_ID = C.CUSTOMER_ID """,
    column_list=[
        Column('customer_blacklist_id', 'INT', uniqueness=True, key=True),
        Column('customer_id', 'INT', key=True),
        Column('customer_blacklist_reason_id', 'INT'),
        Column('administrator_id', 'INT'),
        Column('comment', 'VARCHAR(255)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
    ],
    watermark_column='datetime_added',
)
