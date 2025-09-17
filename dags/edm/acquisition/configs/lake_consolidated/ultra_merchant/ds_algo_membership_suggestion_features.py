from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="ds_algo_membership_suggestion_features",
    company_join_sql="""
        SELECT DISTINCT
            L.ds_algo_membership_suggestion_features_id,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.MEMBERSHIP AS M
            ON DS.STORE_ID = M.STORE_ID
        INNER JOIN {database}.{schema}.membership_suggestion AS ms
         ON ms.MEMBERSHIP_ID = M.MEMBERSHIP_ID
        INNER JOIN {database}.{source_schema}.ds_algo_membership_suggestion_features AS L
            ON L.membership_suggestion_id = ms.membership_suggestion_id """,
    column_list=[
        Column(
            "ds_algo_membership_suggestion_features_id",
            "INT",
            uniqueness=True,
            key=True,
        ),
        Column("membership_suggestion_id", "INT", key=True),
        Column("ds_algo_registry_version_id", "INT", key=True),
        Column("feature_body", "VARCHAR"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_modified",
)
