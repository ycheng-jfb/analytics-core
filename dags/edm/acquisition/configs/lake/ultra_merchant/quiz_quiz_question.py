from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="quiz_quiz_question",
    schema_version_prefix="v2",
    column_list=[
        Column("quiz_quiz_question_id", "INT", uniqueness=True),
        Column("quiz_id", "INT"),
        Column("quiz_question_id", "INT"),
        Column("quiz_question_category_id", "INT"),
        Column("sequence_number", "INT"),
        Column("page_number", "INT"),
        Column("form_control_type", "VARCHAR(25)"),
        Column("question_text", "VARCHAR(255)"),
        Column("answer_required", "INT"),
        Column("disable_children_unless_answered", "INT"),
    ],
)
