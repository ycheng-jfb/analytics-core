from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='administrator_survey_question_answer',
    column_list=[
        Column('administrator_survey_question_answer_id', 'INT', uniqueness=True),
        Column('administrator_survey_id', 'INT'),
        Column('survey_question_id', 'INT'),
        Column('survey_question_answer_id', 'INT'),
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
