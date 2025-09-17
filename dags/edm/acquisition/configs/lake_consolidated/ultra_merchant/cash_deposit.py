from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="cash_deposit",
    company_join_sql="""
     SELECT DISTINCT
         L.CASH_DEPOSIT_ID ,
         DS.COMPANY_ID
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{source_schema}.cash_deposit AS L
     ON DS.STORE_ID = L.STORE_ID """,
    column_list=[
        Column("cash_deposit_id", "INT", uniqueness=True, key=True),
        Column("store_id", "INT"),
        Column("amount", "NUMBER(19, 4)"),
        Column("administrator_id", "INT"),
        Column("deposited_administrator_id", "INT"),
        Column("deposit_image", "VARCHAR(255)"),
        Column("statuscode", "INT"),
        Column("date_deposit", "DATE"),
        Column("datetime_deposited", "TIMESTAMP_NTZ(3)"),
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
