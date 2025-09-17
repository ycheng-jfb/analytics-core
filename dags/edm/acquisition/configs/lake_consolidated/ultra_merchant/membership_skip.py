from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='membership_skip',
    company_join_sql="""
        SELECT DISTINCT
            L.MEMBERSHIP_SKIP_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.MEMBERSHIP AS M
            ON DS.STORE_ID = M.STORE_ID
        INNER JOIN {database}.{source_schema}.membership_skip AS L
            ON L.MEMBERSHIP_ID = M.MEMBERSHIP_ID """,
    column_list=[
        Column('membership_skip_id', 'INT', uniqueness=True, key=True),
        Column('membership_id', 'INT', key=True),
        Column('period_id', 'INT'),
        Column('membership_skip_reason_id', 'INT'),
        Column('reason_comment', 'VARCHAR(255)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('session_id', 'INT', key=True),
    ],
    watermark_column='datetime_modified',
)
