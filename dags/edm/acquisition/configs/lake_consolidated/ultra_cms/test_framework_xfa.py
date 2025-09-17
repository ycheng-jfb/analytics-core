from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="test_framework_xfa",
    schema="ultra_cms",
    company_join_sql="""
        SELECT DISTINCT
            L.test_framework_xfa_id,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{source_schema}.test_framework_xfa AS L
            ON DS.STORE_GROUP_ID = L.STORE_GROUP_ID """,
    column_list=[
        Column("test_framework_xfa_id", "INT", uniqueness=True, key=True),
        Column("store_group_id", "INT"),
        Column("label", "VARCHAR(50)"),
        Column("xfa", "VARCHAR(50)"),
        Column("xfa_sort", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)"),
    ],
    watermark_column="datetime_modified",
)
