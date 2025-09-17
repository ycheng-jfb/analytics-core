from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="customer_fitmatch_product",
    company_join_sql="""
     SELECT DISTINCT
         L.customer_fitmatch_product_id,
         DS.COMPANY_ID
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{schema}.customer_fitmatch AS CF
     ON DS.STORE_GROUP_ID = CF.STORE_GROUP_ID
     INNER JOIN {database}.{source_schema}.customer_fitmatch_product AS L
     ON CF.customer_fitmatch_id=L.customer_fitmatch_id """,
    column_list=[
        Column("customer_fitmatch_product_id", "INT", uniqueness=True, key=True),
        Column("customer_fitmatch_id", "INT", key=True),
        Column("product_id", "INT", key=True),
        Column("sku", "VARCHAR(50)"),
        Column("size", "VARCHAR(50)"),
        Column("style", "VARCHAR(50)"),
        Column("score", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("product_category_id", "INT"),
    ],
    watermark_column="datetime_modified",
)
