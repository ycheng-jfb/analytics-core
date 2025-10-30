from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='order_credit',
    company_join_sql="""
        SELECT DISTINCT
            L.ORDER_CREDIT_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}."ORDER" AS O
            ON DS.STORE_ID = O.STORE_ID
        INNER JOIN {database}.{source_schema}.order_credit AS L
            ON O.ORDER_ID = L.ORDER_ID""",
    column_list=[
        Column('order_credit_id', 'INT', uniqueness=True, key=True),
        Column('order_id', 'INT', key=True),
        Column('gift_certificate_id', 'INT', key=True),
        Column('store_credit_id', 'INT', key=True),
        Column('amount', 'NUMBER(19, 4)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('subtotal', 'NUMBER(19, 4)'),
        Column('shipping', 'NUMBER(19, 4)'),
        Column('tax', 'NUMBER(19, 4)'),
        Column('membership_token_id', 'INT', key=True),
    ],
    watermark_column='datetime_modified',
)
