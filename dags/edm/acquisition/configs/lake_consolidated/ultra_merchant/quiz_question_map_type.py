# TODO parent_tag_id key=true
from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="quiz_question_map_type",
    company_join_sql="""
    SELECT DISTINCT
        L.quiz_question_map_type_id,
        DS.company_id
    FROM {database}.REFERENCE.DIM_STORE AS DS
    INNER JOIN {database}.{source_schema}.quiz_question_map_type AS L
        ON L.STORE_GROUP_ID = DS.STORE_GROUP_ID""",
    column_list=[
        Column("quiz_question_map_type_id", "INT", uniqueness=True, key=True),
        Column("store_group_id", "INT"),
        Column("type", "VARCHAR(25)"),
        Column("label", "VARCHAR(50)"),
        Column("parent_tag_id", "INT", key=True),
        Column("customer_detail_name", "VARCHAR(50)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_added",
)
