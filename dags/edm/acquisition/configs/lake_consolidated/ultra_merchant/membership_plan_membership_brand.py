from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="membership_plan_membership_brand",
    company_join_sql="""
     SELECT DISTINCT
         L.membership_plan_membership_brand_id,
         DS.company_id
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{schema}.membership_plan AS M
      ON DS.STORE_ID= M.STORE_ID
     INNER JOIN {database}.{source_schema}.membership_plan_membership_brand AS L
     ON L.MEMBERSHIP_PLAN_ID=M.MEMBERSHIP_PLAN_ID""",
    column_list=[
        Column("membership_plan_membership_brand_id", "INT", uniqueness=True, key=True),
        Column("membership_plan_id", "INT", key=True),
        Column("membership_brand_id", "INT"),
        Column("store_id", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("price", "NUMBER(19, 4)"),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_added",
)
