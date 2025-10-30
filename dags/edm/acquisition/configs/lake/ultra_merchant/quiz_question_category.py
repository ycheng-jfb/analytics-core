from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='quiz_question_category',
    watermark_column='datetime_added',
    schema_version_prefix='v2',
    column_list=[
        Column('quiz_question_category_id', 'INT', uniqueness=True),
        Column('label', 'VARCHAR(50)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
