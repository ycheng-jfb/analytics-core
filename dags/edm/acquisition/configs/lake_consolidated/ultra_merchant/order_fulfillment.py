from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='order_fulfillment',
    watermark_column='datetime_modified',
    company_join_sql="""
    SELECT DISTINCT
        L.order_fulfillment_id,
        DS.company_id
    FROM {database}.REFERENCE.DIM_STORE AS DS
    INNER JOIN {database}.{schema}."ORDER" AS O
        ON DS.STORE_ID = O.STORE_ID
    INNER JOIN {database}.{source_schema}.order_fulfillment AS L
        ON L.ORDER_ID = O.ORDER_ID""",
    column_list=[
        Column('order_fulfillment_id', 'INT', uniqueness=True, key=True),
        Column('order_id', 'INT', key=True),
        Column('fulfillment_partner_id', 'INT'),
        Column('warehouse_id', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('statuscode', 'INT'),
    ],
)
