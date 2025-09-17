from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="membership_passive_cancels",
    company_join_sql="""
     SELECT DISTINCT
         L.membership_passive_cancels_id,
         DS.company_id
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{schema}.membership AS M
      ON DS.STORE_ID= M.STORE_ID
     INNER JOIN {database}.{source_schema}.membership_passive_cancels AS L
     ON L.MEMBERSHIP_ID=M.MEMBERSHIP_ID""",
    column_list=[
        Column("membership_passive_cancels_id", "INT", uniqueness=True, key=True),
        Column("membership_id", "INT", key=True),
        Column("membership_type_id", "INT"),
        Column("membership_plan_id", "INT", key=True),
        Column("membership_level_id", "INT"),
        Column("membership_statuscode", "INT"),
        Column("curr_period_id", "INT"),
        Column("prev_period_id", "INT"),
        Column("source_table", "VARCHAR(255)"),
        Column("downgrade_status", "VARCHAR(20)"),
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
