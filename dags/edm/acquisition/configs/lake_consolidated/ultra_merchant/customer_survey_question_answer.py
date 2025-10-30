from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='customer_survey_question_answer',
    company_join_sql="""
       SELECT DISTINCT
           L.customer_survey_question_answer_id,
           DS.company_id
       FROM {database}.REFERENCE.DIM_STORE AS DS
       INNER JOIN {database}.{schema}.CUSTOMER AS C
           ON DS.STORE_GROUP_ID = C.STORE_GROUP_ID
       INNER JOIN {database}.{schema}.customer_survey AS CS
           ON CS.CUSTOMER_ID = C.CUSTOMER_ID
         INNER JOIN {database}.{source_schema}.customer_survey_question_answer AS L
         ON L.customer_survey_id=CS.customer_survey_id """,
    column_list=[
        Column('customer_survey_question_answer_id', 'INT', uniqueness=True, key=True),
        Column('customer_survey_id', 'INT', key=True),
        Column('survey_question_id', 'INT', key=True),
        Column('survey_question_answer_id', 'INT', key=True),
        Column('answer_text', 'VARCHAR(2000)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
    ],
    watermark_column='datetime_modified',
)
