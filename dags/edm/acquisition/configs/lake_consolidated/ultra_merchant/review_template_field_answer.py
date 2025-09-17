from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="review_template_field_answer",
    company_join_sql="""
    SELECT DISTINCT
        L.review_template_field_answer_id,
        DS.company_id
    FROM {database}.REFERENCE.DIM_STORE AS DS
    INNER JOIN {database}.{schema}.REVIEW_TEMPLATE AS RT
        ON RT.STORE_GROUP_ID = DS.STORE_GROUP_ID
    INNER JOIN {database}.{schema}.REVIEW_TEMPLATE_FIELD AS RF
        ON RF.REVIEW_TEMPLATE_ID = RT.REVIEW_TEMPLATE_ID
    INNER JOIN {database}.{source_schema}.review_template_field_answer AS L
        ON L.REVIEW_TEMPLATE_FIELD_ID = RF.REVIEW_TEMPLATE_FIELD_ID""",
    column_list=[
        Column("review_template_field_answer_id", "INT", uniqueness=True, key=True),
        Column("review_template_field_id", "INT", key=True),
        Column("label", "VARCHAR(255)"),
        Column("score", "DOUBLE"),
        Column("boolean_value", "INT"),
        Column("enable_comment", "INT"),
        Column("sort", "INT"),
        Column("active", "INT"),
    ],
)
