from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="case_comment",
    company_join_sql="""
     SELECT DISTINCT
         L.CASE_COMMENT_ID,
         DS.COMPANY_ID
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{schema}.CASE AS C
     ON DS.STORE_GROUP_ID = C.STORE_GROUP_ID
     INNER JOIN {database}.{source_schema}.case_comment AS L
     ON L.CASE_ID=C.CASE_ID
     UNION
     SELECT DISTINCT
          L.CASE_COMMENT_ID,
          DS.COMPANY_ID
      FROM {database}.REFERENCE.DIM_STORE AS DS
      INNER JOIN {database}.{schema}.administrator_store_group AS asg
      ON DS.STORE_GROUP_ID = ASG.STORE_GROUP_ID
      INNER JOIN {database}.{source_schema}.case_comment AS L
      ON L.administrator_id=ASG.administrator_id
      """,
    column_list=[
        Column("case_comment_id", "INT", uniqueness=True),
        Column("case_id", "INT", key=True),
        Column("case_comment_type_id", "INT"),
        Column("comment", "VARCHAR(8000)"),
        Column("administrator_id", "INT"),
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
