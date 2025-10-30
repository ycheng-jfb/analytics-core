from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='review_template_field_answer',
    schema_version_prefix='v2',
    column_list=[
        Column('review_template_field_answer_id', 'INT', uniqueness=True),
        Column('review_template_field_id', 'INT'),
        Column('label', 'VARCHAR(255)'),
        Column('score', 'DOUBLE'),
        Column('boolean_value', 'INT'),
        Column('enable_comment', 'INT'),
        Column('sort', 'INT'),
        Column('active', 'INT'),
    ],
)
