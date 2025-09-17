from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table="meta_data",
    table_type=TableType.REGULAR_GLOBAL,
    company_join_sql="""
    SELECT DISTINCT
        L.META_DATA_id,
        DS.company_id
    FROM {database}.{source_schema}.meta_data AS L
    JOIN (
        SELECT DISTINCT company_id
        FROM {database}.REFERENCE.DIM_STORE
        WHERE company_id IS NOT NULL
        ) AS DS""",
    column_list=[
        Column("meta_data_id", "INT", uniqueness=True, key=True),
        Column("object", "VARCHAR(50)"),
        Column("object_id", "INT"),
        Column("html_content", "VARCHAR"),
        Column("field", "VARCHAR(50)"),
        Column("field_content", "VARCHAR"),
    ],
)
