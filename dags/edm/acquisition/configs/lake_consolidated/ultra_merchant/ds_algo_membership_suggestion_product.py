from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="ds_algo_membership_suggestion_product",
    company_join_sql="""
        SELECT DISTINCT
            L.DS_ALGO_MEMBERSHIP_SUGGESTION_PRODUCT_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.MEMBERSHIP AS M
            ON DS.STORE_ID = M.STORE_ID
        INNER JOIN {database}.{schema}.MEMBERSHIP_SUGGESTION AS MS
            ON MS.MEMBERSHIP_ID = M.MEMBERSHIP_ID
        INNER JOIN {database}.{source_schema}.ds_algo_membership_suggestion_product AS L
            ON L.MEMBERSHIP_SUGGESTION_ID = MS.MEMBERSHIP_SUGGESTION_ID """,
    column_list=[
        Column(
            "ds_algo_membership_suggestion_product_id", "INT", uniqueness=True, key=True
        ),
        Column("membership_suggestion_id", "INT", key=True),
        Column("ds_algo_registry_version_id", "INT", key=True),
        Column("product_id", "INT", key=True),
        Column("ordinal", "INT"),
        Column("ds_algo_membership_suggestion_product_type_id", "INT"),
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
