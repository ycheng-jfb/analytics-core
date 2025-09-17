from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="survey",
    company_join_sql="""
        SELECT DISTINCT
            L.SURVEY_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{source_schema}.survey AS L
            ON DS.STORE_GROUP_ID = L.STORE_GROUP_ID """,
    column_list=[
        Column("survey_id", "INT", uniqueness=True, key=True),
        Column("store_group_id", "INT"),
        Column("survey_type_id", "INT"),
        Column("label", "VARCHAR(50)"),
        Column("page_count", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("statuscode", "INT"),
        Column("title", "VARCHAR(255)"),
        Column("introduction", "VARCHAR(512)"),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_modified",
)
