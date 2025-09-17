from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="review_template_field",
    company_join_sql="""
    SELECT DISTINCT
        L.review_template_field_id,
        DS.company_id
    FROM {database}.REFERENCE.DIM_STORE AS DS
    INNER JOIN {database}.{schema}.REVIEW_TEMPLATE AS RT
        ON RT.STORE_GROUP_ID = DS.STORE_GROUP_ID
    INNER JOIN {database}.{source_schema}.review_template_field AS L
        ON L.REVIEW_TEMPLATE_ID = RT.REVIEW_TEMPLATE_ID""",
    column_list=[
        Column("review_template_field_id", "INT", uniqueness=True, key=True),
        Column("review_template_id", "INT", key=True),
        Column("review_template_field_type_id", "INT"),
        Column("review_form_control_type_id", "INT"),
        Column("review_template_field_group_id", "INT", key=True),
        Column("label", "VARCHAR(255)"),
        Column("description", "VARCHAR(512)"),
        Column("page_number", "INT"),
        Column("enable_comment", "INT"),
        Column("internal_use_only", "INT"),
        Column("required", "INT"),
        Column("placement_manual", "INT"),
        Column("sort", "INT"),
        Column("active", "INT"),
        Column("special_field", "INT"),
        Column("smarter_remarketer", "BOOLEAN"),
        Column("fireworks", "INT"),
    ],
)
