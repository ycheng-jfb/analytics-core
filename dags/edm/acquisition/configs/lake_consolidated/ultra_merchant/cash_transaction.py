from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="cash_transaction",
    company_join_sql="""
     SELECT DISTINCT
         L.CASH_TRANSACTION_ID,
         DS.COMPANY_ID
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{schema}.CASH_DRAWER AS CD
     ON DS.STORE_ID = CD.STORE_ID
     INNER JOIN {database}.{source_schema}.cash_transaction AS L
     ON L.CASH_DRAWER_ID=CD.CASH_DRAWER_ID """,
    column_list=[
        Column("cash_transaction_id", "INT", uniqueness=True, key=True),
        Column("amount", "NUMBER(19, 4)"),
        Column("balance", "NUMBER(19, 4)"),
        Column("object", "VARCHAR(50)"),
        Column("object_id", "INT"),
        Column("parent_cash_transaction_id", "INT", key=True),
        Column("administrator_id", "INT"),
        Column("cash_drawer_id", "INT", key=True),
        Column("statuscode", "INT"),
        Column("memo", "VARCHAR(255)"),
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
