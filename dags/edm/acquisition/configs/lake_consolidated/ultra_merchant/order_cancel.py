from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='order_cancel',
    company_join_sql="""
        SELECT DISTINCT
            L.ORDER_CANCEL_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}."ORDER" AS O
            ON DS.STORE_ID = O.STORE_ID
        INNER JOIN {database}.{source_schema}.order_cancel AS L
            ON O.ORDER_ID = L.ORDER_ID""",
    column_list=[
        Column('order_cancel_id', 'INT', uniqueness=True, key=True),
        Column('order_id', 'INT', key=True),
        Column('administrator_id', 'INT'),
        Column('order_cancel_reason_id', 'INT'),
        Column('reason_comment', 'VARCHAR(255)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('datetime_resolved', 'TIMESTAMP_NTZ(3)'),
        Column('statuscode', 'INT'),
    ],
    watermark_column='datetime_modified',
)
