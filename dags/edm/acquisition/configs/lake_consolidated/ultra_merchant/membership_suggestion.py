from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="membership_suggestion",
    company_join_sql="""
        SELECT DISTINCT
            L.MEMBERSHIP_SUGGESTION_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.MEMBERSHIP AS M
            ON DS.STORE_ID = M.STORE_ID
        INNER JOIN {database}.{source_schema}.membership_suggestion AS L
            ON L.MEMBERSHIP_ID = M.MEMBERSHIP_ID """,
    column_list=[
        Column("membership_suggestion_id", "INT", uniqueness=True, key=True),
        Column("membership_id", "INT", key=True),
        Column("membership_suggestion_type_id", "INT"),
        Column("membership_suggestion_method_id", "INT"),
        Column("period_id", "INT"),
        Column("viewed", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("statuscode", "INT"),
    ],
    watermark_column="datetime_modified",
)
