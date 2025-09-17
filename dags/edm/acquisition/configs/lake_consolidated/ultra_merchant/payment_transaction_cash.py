from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="payment_transaction_cash",
    company_join_sql="""
       SELECT DISTINCT
           L.PAYMENT_TRANSACTION_ID,
           DS.COMPANY_ID
       FROM {database}.REFERENCE.DIM_STORE AS DS
       INNER JOIN {database}.{schema}."ORDER" AS C
           ON DS.STORE_ID = C.STORE_ID
       INNER JOIN {database}.{source_schema}.payment_transaction_cash AS L
           ON L.ORDER_ID = C.ORDER_ID """,
    column_list=[
        Column("payment_transaction_id", "INT", uniqueness=True, key=True),
        Column("order_id", "INT", key=True),
        Column("work_location_id", "VARCHAR(50)"),
        Column("transaction_type", "VARCHAR(25)"),
        Column("amount", "NUMBER(19, 4)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("statuscode", "INT"),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("amount_tendered", "NUMBER(19, 4)"),
        Column("administrator_id", "INT"),
    ],
    watermark_column="datetime_modified",
)
