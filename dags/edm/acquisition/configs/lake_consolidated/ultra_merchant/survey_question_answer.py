from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table='survey_question_answer',
    table_type=TableType.REGULAR_GLOBAL,
    company_join_sql="""
     SELECT DISTINCT
         L.survey_question_answer_id,
         DS.COMPANY_ID
     FROM {database}.{source_schema}.survey_question_answer AS L
     JOIN (
          SELECT DISTINCT company_id
          FROM {database}.REFERENCE.DIM_STORE
          WHERE company_id IS NOT NULL
          ) AS DS """,
    column_list=[
        Column('survey_question_answer_id', 'INT', uniqueness=True, key=True),
        Column('survey_question_id', 'INT', key=True),
        Column('answer_text', 'VARCHAR(255)'),
        Column('sequence_number', 'INT'),
        Column('score', 'INT'),
        Column('enable_comment', 'INT'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('statuscode', 'INT'),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
    ],
    watermark_column='datetime_modified',
)
