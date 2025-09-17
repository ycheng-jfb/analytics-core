from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='order_shipping',
    company_join_sql="""
       SELECT DISTINCT
           L.ORDER_SHIPPING_ID,
           DS.COMPANY_ID
       FROM {database}.REFERENCE.DIM_STORE AS DS
       INNER JOIN {database}.{schema}."ORDER" AS C
           ON DS.STORE_ID = C.STORE_ID
       INNER JOIN {database}.{source_schema}.order_shipping AS L
           ON L.ORDER_ID = C.ORDER_ID """,
    column_list=[
        Column('order_shipping_id', 'INT', uniqueness=True, key=True),
        Column('order_id', 'INT', key=True),
        Column('order_line_id', 'INT', key=True),
        Column('order_offer_id', 'INT'),
        Column('shipping_option_id', 'INT'),
        Column('carrier_service_id', 'INT'),
        Column('carrier_rate_id', 'INT'),
        Column('level', 'VARCHAR(15)'),
        Column('type', 'VARCHAR(15)'),
        Column('cost', 'NUMBER(19, 4)'),
        Column('amount', 'NUMBER(19, 4)'),
    ],
    watermark_column='order_shipping_id',
)
