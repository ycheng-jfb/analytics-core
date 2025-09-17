from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="membership_snooze",
    company_join_sql="""
        SELECT DISTINCT
            L.MEMBERSHIP_SNOOZE_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.MEMBERSHIP AS M
            ON DS.STORE_ID = M.STORE_ID
        INNER JOIN {database}.{source_schema}.membership_snooze AS L
            ON L.MEMBERSHIP_ID = M.MEMBERSHIP_ID """,
    column_list=[
        Column("membership_snooze_id", "INT", uniqueness=True, key=True),
        Column("membership_id", "INT", key=True),
        Column("administrator_id", "INT"),
        Column("extended_by_administrator_id", "INT"),
        Column("periods_to_skip", "INT"),
        Column("extended", "INT"),
        Column("comment", "VARCHAR(255)"),
        Column("date_start", "TIMESTAMP_NTZ(0)"),
        Column("date_end", "TIMESTAMP_NTZ(0)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("datetime_cancelled", "TIMESTAMP_NTZ(3)"),
        Column("statuscode", "INT"),
        Column("membership_snooze_type_id", "INT"),
    ],
    watermark_column="datetime_modified",
)
