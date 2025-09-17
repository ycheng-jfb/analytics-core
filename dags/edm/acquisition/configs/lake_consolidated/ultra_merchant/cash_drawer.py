from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="cash_drawer",
    company_join_sql="""
     SELECT DISTINCT
         L.CASH_DRAWER_ID,
         DS.COMPANY_ID
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{source_schema}.cash_drawer AS L
     ON DS.STORE_ID = L.STORE_ID """,
    column_list=[
        Column("cash_drawer_id", "INT", uniqueness=True, key=True),
        Column("store_id", "INT"),
        Column("label", "VARCHAR(50)"),
        Column("type", "VARCHAR(50)"),
        Column("statuscode", "INT"),
        Column("cash_statuscode", "INT"),
        Column("retail_printer_id", "INT"),
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
