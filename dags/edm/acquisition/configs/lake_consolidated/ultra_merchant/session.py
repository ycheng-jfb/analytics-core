from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='session',
    company_join_sql="""
        SELECT DISTINCT
            L.SESSION_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{source_schema}.session AS L
            ON DS.STORE_ID = L.STORE_ID """,
    column_list=[
        Column('session_id', 'INT', uniqueness=True, key=True),
        Column('session_hash_id', 'INT'),
        Column('store_id', 'INT'),
        Column('store_domain_id', 'INT', key=True),
        Column('customer_id', 'INT', key=True),
        Column('dm_gateway_id', 'INT', key=True),
        Column('dm_site_id', 'INT', key=True),
        Column('dm_gateway_test_site_id', 'INT', key=True),
        Column('order_tracking_id', 'INT'),
        Column('session_key', 'VARCHAR(50)'),
        Column('ip', 'VARCHAR(15)'),
        Column('statuscode', 'INT'),
        Column('original_store_domain_id', 'INT', key=True),
        Column('membership_level_id', 'INT'),
        Column('date_added', 'DATE'),
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
