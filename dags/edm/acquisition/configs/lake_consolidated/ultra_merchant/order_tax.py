from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='order_tax',
    company_join_sql="""
        SELECT DISTINCT
            L.ORDER_TAX_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}."ORDER" AS O
            ON DS.STORE_ID = O.STORE_ID
         INNER JOIN {database}.{source_schema}.order_tax AS L
         on L.ORDER_ID = O.ORDER_ID """,
    column_list=[
        Column('order_tax_id', 'INT', uniqueness=True, key=True),
        Column('order_id', 'INT', key=True),
        Column('type', 'VARCHAR(25)'),
        Column('tax_rate', 'DOUBLE'),
        Column('amount', 'NUMBER(19, 4)'),
        Column('taxable_amount', 'NUMBER(19, 4)'),
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
