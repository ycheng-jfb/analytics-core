from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='order_line_token',
    company_join_sql="""
        SELECT DISTINCT
            L.ORDER_LINE_TOKEN_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}."ORDER" AS O
            ON DS.STORE_ID = O.STORE_ID
        INNER JOIN {database}.{source_schema}.order_line_token AS L
            ON L.ORDER_ID = O.ORDER_ID""",
    column_list=[
        Column('order_line_token_id', 'INT', uniqueness=True, key=True),
        Column('order_id', 'INT', key=True),
        Column('order_line_id', 'INT', key=True),
        Column('membership_token_id', 'INT', key=True),
        Column('purchase_price', 'NUMBER(19, 4)'),
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
