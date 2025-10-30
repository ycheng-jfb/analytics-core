from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='quiz_question_answer',
    company_join_sql="""
        SELECT DISTINCT
            L.QUIZ_QUESTION_ANSWER_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE DS
        INNER JOIN {database}.{schema}.QUIZ_QUESTION QQ
            ON DS.STORE_GROUP_ID = QQ.STORE_GROUP_ID
        INNER JOIN {database}.{source_schema}.quiz_question_answer L
            ON QQ.QUIZ_QUESTION_ID = L.QUIZ_QUESTION_ID""",
    column_list=[
        Column('quiz_question_answer_id', 'INT', uniqueness=True, key=True),
        Column('quiz_question_id', 'INT', key=True),
        Column('image_content_id', 'INT'),
        Column('customer_detail_value', 'VARCHAR(50)'),
        Column('answer_text', 'VARCHAR(255)'),
        Column('sequence_number', 'INT'),
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
