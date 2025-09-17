from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="rma_refund",
    company_join_sql="""
        SELECT DISTINCT
            L.RMA_ID,
            L.REFUND_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE DS
        INNER JOIN {database}.{schema}."ORDER" O
            ON DS.STORE_ID=O.STORE_ID
        INNER JOIN {database}.{schema}.REFUND R
            ON R.ORDER_ID = O.ORDER_ID INNER JOIN
           {database}.{source_schema}.rma_refund L
            ON L.REFUND_ID = R.REFUND_ID """,
    column_list=[
        Column(
            "rma_id",
            "INT",
            uniqueness=True,
            key=True,
        ),
        Column(
            "refund_id",
            "INT",
            uniqueness=True,
            key=True,
        ),
    ],
    watermark_column="rma_id",
)
