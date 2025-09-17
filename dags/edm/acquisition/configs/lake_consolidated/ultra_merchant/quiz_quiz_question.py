from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="quiz_quiz_question",
    company_join_sql="""
    SELECT DISTINCT
        L.quiz_quiz_question_id,
        DS.company_id
    FROM {database}.REFERENCE.DIM_STORE AS DS
    INNER JOIN {database}.{schema}.QUIZ AS Q
        ON DS.STORE_GROUP_ID = Q.STORE_GROUP_ID
    INNER JOIN {database}.{source_schema}.quiz_quiz_question AS L
        ON L.QUIZ_ID = Q.QUIZ_ID """,
    column_list=[
        Column("quiz_quiz_question_id", "INT", uniqueness=True, key=True),
        Column("quiz_id", "INT", key=True),
        Column("quiz_question_id", "INT", key=True),
        Column("quiz_question_category_id", "INT"),
        Column("sequence_number", "INT"),
        Column("page_number", "INT"),
        Column("form_control_type", "VARCHAR(25)"),
        Column("question_text", "VARCHAR(255)"),
        Column("answer_required", "INT"),
        Column("disable_children_unless_answered", "INT"),
    ],
)
