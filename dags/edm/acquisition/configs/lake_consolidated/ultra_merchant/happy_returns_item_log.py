from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='happy_returns_item_log',
    company_join_sql="""
        SELECT DISTINCT
            L.happy_returns_item_log_id,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}."ORDER" AS O
            ON DS.store_id = O.store_id
         INNER JOIN {database}.{schema}.RMA AS R
         on R.order_id=O.order_id
        INNER JOIN {database}.{source_schema}.happy_returns_item_log AS L
            ON R.rma_id = L.rma_id """,
    column_list=[
        Column('happy_returns_item_log_id', 'INT', uniqueness=True, key=True),
        Column('rma_id', 'INT', key=True),
        Column('order_line_id', 'INT', key=True),
        Column('action', 'VARCHAR(50)'),
        Column('happy_returns_rma_id', 'VARCHAR(50)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
    ],
    watermark_column='datetime_added',
)
