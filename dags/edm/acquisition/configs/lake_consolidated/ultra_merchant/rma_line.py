from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="rma_line",
    company_join_sql="""
    SELECT DISTINCT
        L.rma_line_id,
        DS.company_id
    FROM {database}.REFERENCE.DIM_STORE AS DS
    INNER JOIN {database}.{schema}."ORDER" AS O
        ON O.STORE_ID = DS.STORE_ID
    INNER JOIN {database}.{schema}.RMA AS R
        ON R.ORDER_ID = O.ORDER_ID
    INNER JOIN {database}.{source_schema}.rma_line AS L
         ON L.RMA_ID = R.RMA_ID """,
    column_list=[
        Column("rma_line_id", "INT", uniqueness=True, key=True),
        Column("rma_id", "INT", key=True),
        Column("order_line_id", "INT", key=True),
        Column("product_id", "INT", key=True),
        Column("quantity", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_modified",
)
