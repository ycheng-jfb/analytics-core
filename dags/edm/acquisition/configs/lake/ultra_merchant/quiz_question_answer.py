from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='quiz_question_answer',
    watermark_column='datetime_modified',
    strict_inequality=True,
    schema_version_prefix='v2',
    column_list=[
        Column('quiz_question_answer_id', 'INT', uniqueness=True),
        Column('quiz_question_id', 'INT'),
        Column('image_content_id', 'INT'),
        Column('customer_detail_value', 'VARCHAR(50)'),
        Column('answer_text', 'VARCHAR(255)'),
        Column('sequence_number', 'INT'),
        Column('enable_comment', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('statuscode', 'INT'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
