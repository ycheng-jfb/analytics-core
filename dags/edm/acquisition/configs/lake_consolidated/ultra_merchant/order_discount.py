from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='order_discount',
    company_join_sql="""
        SELECT DISTINCT
            L.ORDER_DISCOUNT_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}."ORDER" AS O
            ON DS.STORE_ID = O.STORE_ID
        INNER JOIN {database}.{source_schema}.order_discount AS L
            ON L.ORDER_ID = O.ORDER_ID""",
    column_list=[
        Column('order_discount_id', 'INT', uniqueness=True, key=True),
        Column('order_id', 'INT', key=True),
        Column('discount_type_id', 'INT'),
        Column('discount_id', 'INT', key=True),
        Column('promo_id', 'INT', key=True),
        Column('applied_to', 'VARCHAR(15)'),
        Column('amount', 'NUMBER(19, 4)'),
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
    post_sql="""
        UPDATE {database}.{schema}.order_discount
        SET discount_id = meta_company_id
        WHERE meta_original_order_discount_id IN (
            SELECT order_discount_id FROM lake_fl.{schema}.order_discount WHERE discount_id = 0
            UNION
            SELECT order_discount_id FROM lake_sxf.{schema}.order_discount WHERE discount_id = 0
            UNION
            SELECT order_discount_id FROM lake_jfb.{schema}.order_discount WHERE discount_id = 0
        ) AND discount_id IS NULL;
    """,
)
