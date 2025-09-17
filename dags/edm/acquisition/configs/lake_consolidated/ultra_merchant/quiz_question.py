from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='quiz_question',
    company_join_sql="""SELECT DISTINCT l.quiz_question_id, ds.company_id
        FROM {database}.REFERENCE.dim_store ds
        join {database}.{source_schema}.quiz_question l
            on ds.store_group_id=l.store_group_id""",
    column_list=[
        Column('quiz_question_id', 'INT', uniqueness=True, key=True),
        Column('store_group_id', 'INT'),
        Column('parent_quiz_question_id', 'INT', key=True),
        Column('quiz_question_type_id', 'INT'),
        Column('quiz_question_map_type_id', 'INT', key=True),
        Column('quiz_question_category_id', 'INT'),
        Column('image_content_id', 'INT'),
        Column('question_text', 'VARCHAR(255)'),
        Column('comment', 'VARCHAR(100)'),
        Column('sequence_number', 'INT'),
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
