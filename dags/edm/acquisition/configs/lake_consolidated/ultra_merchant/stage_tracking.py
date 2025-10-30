from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='stage_tracking',
    company_join_sql="""
        SELECT DISTINCT
            L.STAGE_TRACKING_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.SESSION AS S
            ON DS.STORE_ID = S.STORE_ID
        INNER JOIN {database}.{source_schema}.stage_tracking AS L
            ON L.SESSION_ID = S.SESSION_ID """,
    column_list=[
        Column('stage_tracking_id', 'INT', uniqueness=True, key=True),
        Column('session_id', 'INT', key=True),
        Column('offer_id', 'INT'),
        Column('last_stage_code', 'VARCHAR(25)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('statuscode', 'INT'),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
    ],
    watermark_column='datetime_modified',
)
