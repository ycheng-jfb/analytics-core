from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="review_template",
    company_join_sql="""
    SELECT DISTINCT
        L.review_template_id,
        DS.company_id
    FROM {database}.REFERENCE.DIM_STORE AS DS
    INNER JOIN {database}.{source_schema}.review_template AS L
        ON L.STORE_GROUP_ID = DS.STORE_GROUP_ID""",
    column_list=[
        Column("review_template_id", "INT", uniqueness=True, key=True),
        Column("store_group_id", "INT"),
        Column("label", "VARCHAR(255)"),
        Column("description", "VARCHAR(512)"),
        Column("pages", "INT"),
    ],
)
