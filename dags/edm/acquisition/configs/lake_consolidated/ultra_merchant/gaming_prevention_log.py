from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='gaming_prevention_log',
    company_join_sql="""
       SELECT DISTINCT
           L.GAMING_PREVENTION_LOG_ID,
           DS.company_id
       FROM {database}.REFERENCE.DIM_STORE AS DS
       INNER JOIN {database}.{schema}."ORDER" AS O
           ON DS.STORE_ID = O.STORE_ID
       INNER JOIN {database}.{source_schema}.gaming_prevention_log AS L
           ON L.ORDER_ID = O.ORDER_ID """,
    column_list=[
        Column('gaming_prevention_log_id', 'INT', uniqueness=True, key=True),
        Column('order_id', 'INT', key=True),
        Column('review_threshold', 'NUMBER(18, 4)'),
        Column('void_threshold', 'NUMBER(18, 4)'),
        Column('total_score', 'NUMBER(18, 4)'),
        Column('gaming_prevention_action', 'VARCHAR(50)'),
        Column('gms_agent_action', 'VARCHAR(50)'),
        Column('gms_agent_action_datetime_updated', 'TIMESTAMP_NTZ(3)'),
        Column('processing_time', 'INT'),
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
