from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='quiz_question',
    watermark_column='datetime_modified',
    strict_inequality=True,
    schema_version_prefix='v2',
    column_list=[
        Column('quiz_question_id', 'INT', uniqueness=True),
        Column('store_group_id', 'INT'),
        Column('parent_quiz_question_id', 'INT'),
        Column('quiz_question_type_id', 'INT'),
        Column('quiz_question_map_type_id', 'INT'),
        Column('quiz_question_category_id', 'INT'),
        Column('image_content_id', 'INT'),
        Column('question_text', 'VARCHAR(255)'),
        Column('comment', 'VARCHAR(100)'),
        Column('sequence_number', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('statuscode', 'INT'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
