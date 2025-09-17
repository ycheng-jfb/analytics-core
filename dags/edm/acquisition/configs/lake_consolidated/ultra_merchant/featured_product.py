from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="featured_product",
    company_join_sql="""
       SELECT DISTINCT
           L.featured_product_id,
           DS.company_id
       FROM {database}.REFERENCE.DIM_STORE AS DS
       INNER JOIN {database}.{schema}.PRODUCT AS p
           ON DS.STORE_GROUP_ID = p.STORE_GROUP_ID
       INNER JOIN {database}.{source_schema}.featured_product AS L
           ON L.PRODUCT_ID = p.PRODUCT_ID """,
    column_list=[
        Column("featured_product_id", "INT", uniqueness=True, key=True),
        Column("featured_product_location_id", "INT", key=True),
        Column("object", "VARCHAR(50)"),
        Column("object_id", "INT"),
        Column("product_id", "INT", key=True),
        Column("sort", "INT"),
        Column("active", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("datetime_start", "TIMESTAMP_NTZ(3)"),
        Column("datetime_end", "TIMESTAMP_NTZ(3)"),
        Column("sale_price", "NUMBER(19, 4)"),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("pin_product", "INT"),
    ],
    watermark_column="datetime_modified",
)
