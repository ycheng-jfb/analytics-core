from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="product_wait_list",
    company_join_sql="""
    SELECT DISTINCT
        L.product_wait_list_id,
        DS.company_id
    FROM {database}.REFERENCE.DIM_STORE AS DS
    INNER JOIN {database}.{schema}.PRODUCT AS P
        ON DS.STORE_GROUP_ID = P.STORE_GROUP_ID
    INNER JOIN {database}.{source_schema}.product_wait_list AS L
        ON L.PRODUCT_ID = P.PRODUCT_ID """,
    column_list=[
        Column("product_wait_list_id", "INT", uniqueness=True, key=True),
        Column("product_id", "INT", key=True),
        Column("product_wait_list_type_id", "INT"),
        Column("start_datetime", "TIMESTAMP_NTZ(3)"),
        Column("end_datetime", "TIMESTAMP_NTZ(3)"),
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
