from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="refund",
    company_join_sql="""
        SELECT DISTINCT
            L.REFUND_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}."ORDER" AS O
            ON DS.STORE_ID = O.STORE_ID
        INNER JOIN {database}.{source_schema}.refund AS L
            ON L.ORDER_ID = O.ORDER_ID """,
    column_list=[
        Column("refund_id", "INT", uniqueness=True, key=True),
        Column("order_id", "INT", key=True),
        Column("payment_id", "INT", key=True),
        Column("administrator_id", "INT"),
        Column("payment_method", "VARCHAR(25)"),
        Column("payment_transaction_id", "INT", key=True),
        Column("refund_reason_id", "INT"),
        Column("reason_comment", "VARCHAR(255)"),
        Column("product_refund", "NUMBER(19, 4)"),
        Column("shipping_refund", "NUMBER(19, 4)"),
        Column("tax_refund", "NUMBER(19, 4)"),
        Column("total_refund", "NUMBER(19, 4)"),
        Column("approved_administrator_id", "INT"),
        Column("date_added", "TIMESTAMP_NTZ(0)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)"),
        Column("statuscode", "INT"),
        Column("datetime_refunded", "TIMESTAMP_NTZ(3)"),
        Column("datetime_transaction", "TIMESTAMP_NTZ(3)"),
        Column("datetime_local_transaction", "TIMESTAMP_NTZ(3)"),
    ],
    watermark_column="datetime_modified",
)
