from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="customer_tag",
    company_join_sql="""
        SELECT DISTINCT
            L.CUSTOMER_ID,
            L.TAG_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.CUSTOMER AS C
            ON DS.STORE_GROUP_ID = C.STORE_GROUP_ID
        INNER JOIN {database}.{source_schema}.customer_tag AS L
            ON L.CUSTOMER_ID = C.CUSTOMER_ID """,
    column_list=[
        Column("customer_tag_id", "INT", key=True),
        Column("customer_id", "INT", uniqueness=True, key=True),
        Column("tag_id", "INT", uniqueness=True, key=True),
        Column("weight", "DOUBLE"),
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
