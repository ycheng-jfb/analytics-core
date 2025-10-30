from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='exchange',
    company_join_sql="""
       SELECT DISTINCT
           L.exchange_id,
           DS.company_id
       FROM {database}.REFERENCE.DIM_STORE AS DS
       INNER JOIN {database}.{schema}."ORDER" AS o
           ON DS.STORE_ID = O.STORE_ID
       INNER JOIN {database}.{source_schema}.exchange AS L
           ON L.ORIGINAL_ORDER_ID = o.ORDER_ID """,
    column_list=[
        Column('exchange_id', 'INT', uniqueness=True, key=True),
        Column('rma_id', 'INT', key=True),
        Column('original_order_id', 'INT', key=True),
        Column('exchange_order_id', 'INT', key=True),
        Column('session_id', 'INT', key=True),
        Column('refund_return_action_id', 'INT', key=True),
        Column('charge_shipping', 'INT'),
        Column('charge_return_shipping', 'INT'),
        Column('date_added', 'TIMESTAMP_NTZ(0)'),
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
