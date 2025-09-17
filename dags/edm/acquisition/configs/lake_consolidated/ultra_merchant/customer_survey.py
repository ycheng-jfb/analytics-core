from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="customer_survey",
    company_join_sql="""
       SELECT DISTINCT
           L.customer_survey_id,
           DS.company_id
       FROM {database}.REFERENCE.DIM_STORE AS DS
       INNER JOIN {database}.{schema}.CUSTOMER AS C
           ON DS.STORE_GROUP_ID = C.STORE_GROUP_ID
       INNER JOIN {database}.{source_schema}.customer_survey AS L
           ON L.CUSTOMER_ID = C.CUSTOMER_ID """,
    column_list=[
        Column("customer_survey_id", "INT", uniqueness=True, key=True),
        Column("customer_id", "INT", key=True),
        Column("survey_id", "INT", key=True),
        Column("subscription_id", "INT"),
        Column("session_id", "INT", key=True),
        Column("page_number", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("datetime_completed", "TIMESTAMP_NTZ(3)"),
        Column("statuscode", "INT"),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_modified",
)
