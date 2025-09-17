from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='session_tracking',
    company_join_sql="""
        SELECT DISTINCT
            L.SESSION_TRACKING_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.SESSION AS S
            ON DS.STORE_ID = S.STORE_ID
        INNER JOIN {database}.{schema}.SESSION_TRACKING_DETAIL AS ST
            ON S.SESSION_ID = ST.SESSION_ID
        INNER JOIN {database}.{source_schema}.SESSION_TRACKING AS L
            ON L.SESSION_TRACKING_ID = ST.SESSION_TRACKING_ID """,
    column_list=[
        Column('session_tracking_id', 'INT', uniqueness=True, key=True),
        Column('code', 'VARCHAR(50)'),
        Column('label', 'VARCHAR(50)'),
        Column('is_unique', 'BOOLEAN'),
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
