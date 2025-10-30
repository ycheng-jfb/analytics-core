from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='visitor_session',
    company_join_sql="""
        SELECT DISTINCT
            L.visitor_session_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.SESSION AS S
            ON DS.STORE_ID = S.STORE_ID
        INNER JOIN {database}.{source_schema}.visitor_session AS L
            ON L.SESSION_ID = S.SESSION_ID """,
    partition_cols=['visitor_session_id', 'visitor_session_hash_id'],
    column_list=[
        Column('visitor_session_id', 'INT', uniqueness=True, key=True),
        Column('visitor_session_hash_id', 'INT'),
        Column('visitor_id', 'INT', key=True),
        Column('session_id', 'INT', key=True),
        Column('datetime_expired', 'TIMESTAMP_NTZ(3)'),
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
