from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='survey_survey_question',
    company_join_sql="""
        SELECT DISTINCT
            L.SURVEY_SURVEY_QUESTION_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.SURVEY AS S
            ON DS.STORE_GROUP_ID = S.STORE_GROUP_ID
        INNER JOIN {database}.{source_schema}.survey_survey_question AS L
            ON L.SURVEY_ID = S.SURVEY_ID """,
    column_list=[
        Column('survey_survey_question_id', 'INT', uniqueness=True, key=True),
        Column('survey_id', 'INT', key=True),
        Column('survey_question_id', 'INT', key=True),
        Column('survey_question_category_id', 'INT'),
        Column('sequence_number', 'INT'),
        Column('page_number', 'INT'),
        Column('form_control_type', 'VARCHAR(25)'),
        Column('question_text', 'VARCHAR(255)'),
        Column('answer_required', 'INT'),
        Column('disable_children_unless_answered', 'INT'),
    ],
)
