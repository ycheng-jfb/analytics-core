from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="quiz",
    company_join_sql="""
    SELECT DISTINCT
        L.quiz_id,
        DS.company_id
    FROM {database}.REFERENCE.DIM_STORE AS DS
    INNER JOIN {database}.{source_schema}.quiz AS L
        ON L.STORE_GROUP_ID = DS.STORE_GROUP_ID""",
    column_list=[
        Column("quiz_id", "INT", uniqueness=True, key=True),
        Column("store_group_id", "INT"),
        Column("quiz_type_id", "INT"),
        Column("root_quiz_id", "INT"),
        Column("label", "VARCHAR(50)"),
        Column("page_count", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("statuscode", "INT"),
    ],
    watermark_column="datetime_added",
)
