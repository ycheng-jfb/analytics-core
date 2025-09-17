from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="membership_suggestion_product_purchase",
    company_join_sql="""
        SELECT DISTINCT
            L.MEMBERSHIP_SUGGESTION_PRODUCT_PURCHASE_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.PRODUCT AS P
            ON DS.STORE_GROUP_ID = P.STORE_GROUP_ID
        INNER JOIN {database}.{source_schema}.membership_suggestion_product_purchase AS L
            ON L.PRODUCT_ID = P.PRODUCT_ID """,
    column_list=[
        Column(
            "membership_suggestion_product_purchase_id",
            "INT",
            uniqueness=True,
            key=True,
        ),
        Column("membership_suggestion_product_id", "INT", key=True),
        Column("order_id", "INT", key=True),
        Column("product_id", "INT", key=True),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_added",
)
