from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='product_tag',
    company_join_sql="""
    SELECT DISTINCT
            L.PRODUCT_ID,
            L.TAG_ID,
            DS.COMPANY_ID
        FROM {database}.{source_schema}.product_tag L
        LEFT JOIN (
            SELECT DISTINCT
                P.PRODUCT_ID,
                COALESCE(P.STORE_GROUP_ID, MS.STORE_GROUP_ID) AS CONCAT_STORE_GROUP_ID
            FROM {database}.{schema}.PRODUCT P
            /* GRAB STORE_GROUP_ID OF MASTER PRODUCTS */
            LEFT JOIN (
                SELECT DISTINCT
                    PRODUCT_ID,
                    STORE_GROUP_ID
                FROM {database}.{schema}.PRODUCT
                WHERE MASTER_PRODUCT_ID IS NULL
                ) AS MS
                ON P.MASTER_PRODUCT_ID = MS.PRODUCT_ID
            ) AS PS
            ON PS.PRODUCT_ID = L.PRODUCT_ID
        LEFT JOIN {database}.REFERENCE.DIM_STORE AS DS
            ON DS.STORE_GROUP_ID = PS.CONCAT_STORE_GROUP_ID""",
    column_list=[
        Column('product_id', 'INT', uniqueness=True, key=True),
        Column('tag_id', 'INT', uniqueness=True, key=True),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)'),
        Column('product_tag_id', 'INT', keep_original=True),
    ],
    watermark_column='datetime_modified',
)
