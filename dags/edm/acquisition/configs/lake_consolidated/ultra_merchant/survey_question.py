from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table="survey_question",
    table_type=TableType.REGULAR_GLOBAL,
    company_join_sql="""
       SELECT DISTINCT
           L.SURVEY_QUESTION_ID,
           DS.COMPANY_ID
       FROM {database}.{source_schema}.survey_question AS L
       JOIN (
            SELECT DISTINCT company_id
            FROM {database}.REFERENCE.DIM_STORE
            WHERE company_id IS NOT NULL
            ) AS DS """,
    column_list=[
        Column("survey_question_id", "INT", uniqueness=True, key=True),
        Column("store_group_id", "INT"),
        Column("parent_survey_question_id", "INT"),
        Column("survey_question_type_id", "INT"),
        Column("survey_question_category_id", "INT"),
        Column("question_text", "VARCHAR(255)"),
        Column("sequence_number", "INT"),
        Column("auto_fail", "BOOLEAN"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("statuscode", "INT"),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_modified",
)
