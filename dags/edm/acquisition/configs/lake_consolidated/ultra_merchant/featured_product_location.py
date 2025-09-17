from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="featured_product_location",
    company_join_sql="""
       SELECT DISTINCT
           L.featured_product_location_id,
           DS.company_id
       FROM {database}.REFERENCE.DIM_STORE AS DS
       INNER JOIN {database}.{source_schema}.featured_product_location AS L
        ON DS.STORE_GROUP_ID = L.STORE_GROUP_ID """,
    column_list=[
        Column("featured_product_location_id", "INT", uniqueness=True, key=True),
        Column("store_group_id", "INT"),
        Column("label", "VARCHAR(100)"),
        Column("max_products", "INT"),
        Column("exclude_products_from_membership_recommendation", "INT"),
        Column("featured_product_location_type_id", "INT"),
        Column("active", "INT"),
        Column("page_title", "VARCHAR(50)"),
        Column("page_url", "VARCHAR(128)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("code", "VARCHAR(50)"),
        Column("membership_brand_id_list", "VARCHAR(50)"),
        Column(
            "datetime_last_refresh",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("featured_product_location_source_id", "INT"),
        Column("statuscode", "INT"),
    ],
    watermark_column="datetime_modified",
)
