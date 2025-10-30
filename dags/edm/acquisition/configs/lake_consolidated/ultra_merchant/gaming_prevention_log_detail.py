from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='gaming_prevention_log_detail',
    company_join_sql="""
       SELECT DISTINCT
           L.GAMING_PREVENTION_LOG_DETAIL_ID,
           DS.company_id
       FROM {database}.REFERENCE.DIM_STORE AS DS
       INNER JOIN {database}.{schema}."ORDER" AS O
           ON DS.STORE_ID = O.STORE_ID
       INNER JOIN {database}.{schema}.GAMING_PREVENTION_LOG AS GPL
           ON GPL.ORDER_ID = O.ORDER_ID
       INNER JOIN {database}.{source_schema}.gaming_prevention_log_detail AS L
           ON L.GAMING_PREVENTION_LOG_ID = GPL.GAMING_PREVENTION_LOG_ID """,
    column_list=[
        Column('gaming_prevention_log_detail_id', 'INT', uniqueness=True, key=True),
        Column('gaming_prevention_log_id', 'INT', key=True),
        Column('gaming_prevention_check_id', 'INT'),
        Column('gaming_prevention_query_id', 'INT'),
        Column('order_id', 'INT', key=True),
        Column('hits', 'INT'),
        Column('base_score', 'NUMBER(18, 4)'),
        Column('boost_factor', 'NUMBER(18, 4)'),
        Column('score', 'NUMBER(18, 4)'),
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
