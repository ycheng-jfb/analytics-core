from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="membership_level_group_modification_log",
    company_join_sql="""
     SELECT DISTINCT
         L.membership_level_group_modification_log_id,
         DS.company_id
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{schema}.membership AS M
      ON DS.STORE_ID= M.STORE_ID
     INNER JOIN {database}.{source_schema}.membership_level_group_modification_log AS L
     ON L.MEMBERSHIP_ID=M.MEMBERSHIP_ID""",
    column_list=[
        Column(
            "membership_level_group_modification_log_id",
            "INT",
            uniqueness=True,
            key=True,
        ),
        Column("membership_id", "INT", key=True),
        Column("old_value", "INT"),
        Column("new_value", "INT"),
        Column("passive_downgrade", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_added",
)
