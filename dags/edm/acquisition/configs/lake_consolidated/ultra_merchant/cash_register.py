from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="cash_register",
    company_join_sql="""
     SELECT DISTINCT
         L.CASH_REGISTER_ID,
         DS.COMPANY_ID
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{source_schema}.cash_register AS L
     ON DS.STORE_ID = L.STORE_ID """,
    column_list=[
        Column("cash_register_id", "INT", uniqueness=True, key=True),
        Column("label", "VARCHAR(255)"),
        Column("store_id", "INT"),
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
