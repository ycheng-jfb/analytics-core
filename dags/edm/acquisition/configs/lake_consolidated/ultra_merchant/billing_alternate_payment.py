from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.REGULAR,
    table="billing_alternate_payment",
    company_join_sql="""
            SELECT DISTINCT
            L.BILLING_ALTERNATE_PAYMENT_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        LEFT JOIN {database}.{source_schema}."ORDER" AS O
            ON DS.STORE_ID = O.STORE_ID
        LEFT JOIN {database}.{source_schema}.BILLING_ALTERNATE_PAYMENT AS L
            ON O.ORDER_ID = L.ORDER_ID
        """,
    column_list=[
        Column("billing_alternate_payment_id", "INT", uniqueness=True, key=True),
        Column("order_id", "INT", key=True),
        Column("type", "VARCHAR(50)"),
        Column("retry_cycle", "NUMBER(38,0)"),
        Column("cards_available", "NUMBER(38,0)"),
        Column("cards_attempted", "NUMBER(38,0)"),
        Column("current_payment_method", "VARCHAR(25)"),
        Column("current_payment_object_id", "NUMBER(38,0)", key=True),
        Column("current_exp_month", "VARCHAR(2)"),
        Column("current_exp_year", "VARCHAR(2)"),
        Column("error_message", "VARCHAR(255)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)"),
        Column("statuscode", "NUMBER(38,0)"),
    ],
    watermark_column="datetime_modified",
)
