from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="case_qa",
    company_join_sql="""
     SELECT DISTINCT
         L.CASE_QA_ID,
         DS.COMPANY_ID
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{schema}.CASE AS C
     ON DS.STORE_GROUP_ID = C.STORE_GROUP_ID
     INNER JOIN {database}.{source_schema}.case_qa AS L
     ON L.CASE_ID=C.CASE_ID """,
    column_list=[
        Column("case_qa_id", "INT", uniqueness=True),
        Column("case_id", "INT", key=True),
        Column("case_qa_scorecard_id", "INT"),
        Column("case_qa_scorecard_total_score", "INT"),
        Column("customer_survey_id", "INT", key=True),
        Column("customer_survey_total_score", "INT"),
        Column("customer_survey_rep_total_score", "INT"),
        Column("customer_survey_infrastructure_total_score", "INT"),
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
