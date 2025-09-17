from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='order_chargeback',
    company_join_sql="""
        SELECT DISTINCT
            L.ORDER_CHARGEBACK_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}."ORDER" AS O
            ON DS.STORE_ID = O.STORE_ID
        INNER JOIN {database}.{source_schema}.order_chargeback AS L
            ON O.ORDER_ID = L.ORDER_ID""",
    column_list=[
        Column('order_chargeback_id', 'INT', uniqueness=True, key=True),
        Column('order_id', 'INT', key=True),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('statuscode', 'INT'),
    ],
    watermark_column='datetime_modified',
)
