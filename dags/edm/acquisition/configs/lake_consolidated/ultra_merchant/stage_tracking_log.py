from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table='stage_tracking_log',
    table_type=TableType.OBJECT_COLUMN_NULL,
    company_join_sql="""
        SELECT DISTINCT
            L.STAGE_TRACKING_LOG_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.SESSION AS S
            ON DS.STORE_ID = S.STORE_ID
        INNER JOIN {database}.{schema}.STAGE_TRACKING AS ST
            ON ST.SESSION_ID = S.SESSION_ID
        INNER JOIN {database}.{source_schema}.stage_tracking_log AS L
            ON L.STAGE_TRACKING_ID = ST.STAGE_TRACKING_ID """,
    column_list=[
        Column('stage_tracking_log_id', 'INT', uniqueness=True, key=True),
        Column('stage_tracking_id', 'INT', key=True),
        Column('stage_code', 'VARCHAR(25)'),
        Column('object', 'VARCHAR(50)'),
        Column('object_id', 'INT'),
        Column('input_errors', 'INT'),
        Column('process_errors', 'INT'),
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
