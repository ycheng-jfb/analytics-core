from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='order_vat',
    company_join_sql="""
        SELECT DISTINCT
            L.ORDER_VAT_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}."ORDER" AS O
            ON DS.STORE_ID = O.STORE_ID
        INNER JOIN {database}.{source_schema}.order_vat AS L
            ON L.ORDER_ID = O.ORDER_ID """,
    column_list=[
        Column('order_vat_id', 'INT', uniqueness=True, key=True),
        Column('order_id', 'INT', key=True),
        Column('vat_rate', 'DOUBLE'),
        Column('amount', 'NUMBER(19, 4)'),
        Column('deferred_tax', 'NUMBER(19, 4)'),
        Column('item_vat', 'NUMBER(19, 4)'),
        Column('shipping_vat', 'NUMBER(19, 4)'),
        Column('vat_credited', 'NUMBER(19, 4)'),
        Column('deferred_credit_vat_country', 'INT'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('country_code', 'VARCHAR(2)'),
    ],
    watermark_column='datetime_modified',
)
