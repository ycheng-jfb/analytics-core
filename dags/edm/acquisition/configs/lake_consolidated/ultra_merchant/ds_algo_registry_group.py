from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="ds_algo_registry_group",
    company_join_sql="""
        SELECT DISTINCT
            L.DS_ALGO_REGISTRY_GROUP_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{source_schema}.ds_algo_registry_group AS L
            ON DS.STORE_GROUP_ID = L.STORE_GROUP_ID """,
    column_list=[
        Column("ds_algo_registry_group_id", "INT", uniqueness=True, key=True),
        Column("label", "VARCHAR(100)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("store_group_id", "INT"),
    ],
    watermark_column="datetime_modified",
)
