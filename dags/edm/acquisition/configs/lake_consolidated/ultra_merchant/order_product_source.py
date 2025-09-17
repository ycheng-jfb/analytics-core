from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.VALUE_COLUMN,
    table='order_product_source',
    company_join_sql="""
       SELECT DISTINCT
           L.ORDER_PRODUCT_SOURCE_ID,
           DS.COMPANY_ID
       FROM {database}.REFERENCE.DIM_STORE AS DS
       INNER JOIN {database}.{schema}."ORDER" AS C
           ON DS.STORE_ID = C.STORE_ID
       INNER JOIN {database}.{source_schema}.order_product_source AS L
           ON L.ORDER_ID = C.ORDER_ID """,
    column_list=[
        Column('order_product_source_id', 'INT', uniqueness=True, key=True),
        Column('order_id', 'INT', key=True),
        Column('product_id', 'INT', key=True),
        Column('value', 'VARCHAR(200)'),
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
