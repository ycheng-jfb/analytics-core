from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="quiz_question_answer_product",
    schema_version_prefix="v2",
    column_list=[
        Column("quiz_question_answer_id", "INT", uniqueness=True),
        Column("product_id", "INT", uniqueness=True),
        Column("weight", "DOUBLE"),
    ],
)
