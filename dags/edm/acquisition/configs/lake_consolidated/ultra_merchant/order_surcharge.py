from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='order_surcharge',
    company_join_sql="""
       SELECT DISTINCT
           L.ORDER_SURCHARGE_ID,
           DS.COMPANY_ID
       FROM {database}.REFERENCE.DIM_STORE AS DS
       INNER JOIN {database}.{schema}."ORDER" AS C
           ON DS.STORE_ID = C.STORE_ID
       INNER JOIN {database}.{source_schema}.order_surcharge AS L
           ON L.ORDER_ID = C.ORDER_ID """,
    column_list=[
        Column('order_surcharge_id', 'INT', uniqueness=True, key=True),
        Column('order_id', 'INT', key=True),
        Column('type', 'VARCHAR(25)'),
        Column('rate', 'DOUBLE'),
        Column('subtotal_before_surcharge', 'NUMBER(19, 4)'),
        Column('surchargeable_amount', 'NUMBER(19, 4)'),
        Column('surcharge_amount_raw', 'NUMBER(19, 4)'),
        Column('surcharge_amount_rounded', 'NUMBER(19, 4)'),
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
