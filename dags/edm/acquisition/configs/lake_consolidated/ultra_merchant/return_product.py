from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='return_product',
    company_join_sql="""
        SELECT DISTINCT
            L.RETURN_PRODUCT_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE  DS
        INNER JOIN {database}.{schema}."ORDER" O
            ON DS.STORE_ID = O.STORE_ID
        INNER JOIN {database}.{schema}.RETURN R
            ON R.ORDER_ID = O.ORDER_ID
        INNER JOIN {database}.{source_schema}.return_product L
            ON L.RETURN_ID = R.RETURN_ID""",
    column_list=[
        Column('return_product_id', 'INT', uniqueness=True, key=True),
        Column('return_id', 'INT', key=True),
        Column('order_line_id', 'INT', key=True),
        Column('rma_product_id', 'INT', key=True),
        Column('product_id', 'INT', key=True),
        Column('item_id', 'INT'),
        Column('expected_return_condition_id', 'INT'),
        Column('return_condition_id', 'INT'),
        Column('return_disposition_id', 'INT'),
        Column('lpn_code', 'VARCHAR(25)'),
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
