from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="request_comment",
    schema="gdpr",
    company_join_sql="""
       SELECT DISTINCT
           L.request_comment_id,
           DS.company_id
       FROM {database}.REFERENCE.DIM_STORE AS DS
       INNER JOIN {database}.ULTRA_MERCHANT.CUSTOMER AS C
           ON DS.STORE_GROUP_ID = C.STORE_GROUP_ID
       INNER JOIN {database}.{schema}.request AS R
           ON R.CUSTOMER_ID = C.CUSTOMER_ID
        INNER JOIN  {database}.{source_schema}.request_comment L
        ON L.request_id= R.request_id""",
    column_list=[
        Column("request_comment_id", "INT", uniqueness=True, key=True),
        Column("request_id", "INT", key=True),
        Column("administrator_id", "INT"),
        Column("comment", "VARCHAR(8000)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)"),
    ],
    watermark_column="datetime_modified",
)
