from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="customer_quiz_question_answer",
    company_join_sql="""
      SELECT DISTINCT
          L.CUSTOMER_QUIZ_QUESTION_ANSWER_ID,
          DS.COMPANY_ID
      FROM {database}.REFERENCE.DIM_STORE AS DS
      INNER JOIN {database}.{schema}.Session  AS S
      ON DS.STORE_ID = S.STORE_ID
      INNER JOIN {database}.{schema}.CUSTOMER_QUIZ AS CQ
      ON S.SESSION_ID = CQ.SESSION_ID
      INNER JOIN {database}.{source_schema}.customer_quiz_question_answer AS L
      ON L.CUSTOMER_QUIZ_ID=CQ.CUSTOMER_QUIZ_ID """,
    column_list=[
        Column("customer_quiz_question_answer_id", "INT", uniqueness=True, key=True),
        Column("customer_quiz_id", "INT", key=True),
        Column("quiz_question_id", "INT", key=True),
        Column("quiz_question_answer_id", "INT", key=True),
        Column("answer_text", "VARCHAR(2000)"),
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
