from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="membership_downgrade",
    company_join_sql="""
        SELECT DISTINCT
            L.MEMBERSHIP_DOWNGRADE_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.MEMBERSHIP AS M
            ON DS.STORE_ID = M.STORE_ID
        INNER JOIN {database}.{source_schema}.membership_downgrade AS L
            ON L.MEMBERSHIP_ID = M.MEMBERSHIP_ID """,
    column_list=[
        Column("membership_downgrade_id", "INT", uniqueness=True, key=True),
        Column("membership_id", "INT", key=True),
        Column("membership_billing_id", "INT", key=True),
        Column("administrator_id", "INT"),
        Column("membership_downgrade_reason_id", "INT"),
        Column("reason_comment", "VARCHAR(255)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("datetime_completed", "TIMESTAMP_NTZ(3)"),
        Column("date_scheduled", "TIMESTAMP_NTZ(0)"),
        Column("statuscode", "INT"),
    ],
    watermark_column="datetime_modified",
)
