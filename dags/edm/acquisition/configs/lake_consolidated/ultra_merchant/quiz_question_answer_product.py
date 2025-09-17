from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='quiz_question_answer_product',
    company_join_sql="""
    SELECT DISTINCT
        L.QUIZ_QUESTION_ANSWER_ID,
        L.PRODUCT_ID,
        DS.COMPANY_ID
    FROM {database}.REFERENCE.DIM_STORE AS DS
    INNER JOIN {database}.{schema}.PRODUCT AS P
        ON DS.STORE_GROUP_ID = P.STORE_GROUP_ID
    INNER JOIN {database}.{source_schema}.quiz_question_answer_product AS L
        ON L.PRODUCT_ID = P.PRODUCT_ID """,
    column_list=[
        Column('quiz_question_answer_id', 'INT', uniqueness=True, key=True),
        Column('product_id', 'INT', uniqueness=True, key=True),
        Column('weight', 'DOUBLE'),
    ],
)
