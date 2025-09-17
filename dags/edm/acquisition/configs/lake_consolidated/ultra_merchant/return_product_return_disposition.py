from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='return_product_return_disposition',
    company_join_sql="""
    SELECT DISTINCT
        L.RETURN_PRODUCT_ID,
        L.RETURN_DISPOSITION_ID,
        DS.COMPANY_ID
    FROM {database}.REFERENCE.DIM_STORE AS DS
    INNER JOIN {database}.{schema}."ORDER" AS O
        ON O.STORE_ID = DS.STORE_ID
    INNER JOIN {database}.{schema}.RETURN AS R
        ON R.ORDER_ID = O.ORDER_ID
    INNER JOIN {database}.{schema}.RETURN_PRODUCT AS RP
        ON RP.RETURN_ID = R.RETURN_ID
    INNER JOIN {database}.{source_schema}.return_product_return_disposition AS L
        ON L.RETURN_PRODUCT_ID = RP.RETURN_PRODUCT_ID """,
    column_list=[
        Column('return_product_id', 'INT', uniqueness=True, key=True),
        Column('return_disposition_id', 'INT', uniqueness=True),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
    ],
    watermark_column='datetime_added',
)
