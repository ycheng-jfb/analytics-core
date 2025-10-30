from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='customer_quiz_question_answer',
    watermark_column='datetime_modified',
    initial_load_value='2020-01-15',
    schema_version_prefix='v2',
    column_list=[
        Column('customer_quiz_question_answer_id', 'INT', uniqueness=True),
        Column('customer_quiz_id', 'INT'),
        Column('quiz_question_id', 'INT'),
        Column('quiz_question_answer_id', 'INT'),
        Column('answer_text', 'VARCHAR(2000)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
