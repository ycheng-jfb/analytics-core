from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="exchange_line",
    company_join_sql="""
       SELECT DISTINCT
           L.EXCHANGE_LINE_ID,
           DS.COMPANY_ID
       FROM {database}.REFERENCE.DIM_STORE AS DS
       INNER JOIN {database}.{schema}."ORDER" AS O
           ON DS.STORE_ID = O.STORE_ID
       INNER JOIN {database}.{schema}.EXCHANGE AS E
           ON E.ORIGINAL_ORDER_ID = O.ORDER_ID
       INNER JOIN {database}.{source_schema}.exchange_line AS L
           ON L.EXCHANGE_ID = E.EXCHANGE_ID """,
    column_list=[
        Column("exchange_line_id", "INT", uniqueness=True, key=True),
        Column("exchange_id", "INT", key=True),
        Column("rma_product_id", "INT", key=True),
        Column("original_product_id", "INT", key=True),
        Column("exchange_product_id", "INT", key=True),
        Column("price_difference", "NUMBER(19, 4)"),
        Column("price", "NUMBER(19, 4)"),
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
