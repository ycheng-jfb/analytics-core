from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.REGULAR,
    table="billing_alternate_payment_log",
    company_join_sql="""
            SELECT DISTINCT
            L.BILLING_ALTERNATE_PAYMENT_LOG_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        LEFT JOIN {database}.{source_schema}."ORDER" AS O
            ON DS.STORE_ID = O.STORE_ID
        LEFT JOIN {database}.{source_schema}.BILLING_ALTERNATE_PAYMENT AS BAP
            ON O.ORDER_ID = BAP.ORDER_ID
        LEFT JOIN {database}.{source_schema}.BILLING_ALTERNATE_PAYMENT_LOG AS L
            ON BAP.BILLING_ALTERNATE_PAYMENT_ID = L.BILLING_ALTERNATE_PAYMENT_ID
        """,
    column_list=[
        Column("billing_alternate_payment_log_id", "INT", uniqueness=True, key=True),
        Column("billing_alternate_payment_id", "INT", key=True),
        Column("order_id", "INT", key=True),
        Column("rank", "NUMBER(38,0)"),
        Column("auth_payment_transaction_id", "NUMBER(38,0)", key=True),
        Column("payment_method", "VARCHAR(25)"),
        Column("payment_object_id", "NUMBER(38,0)", key=True),
        Column("exp_month", "VARCHAR(2)"),
        Column("exp_year", "VARCHAR(2)"),
        Column("card_num_matches_default", "NUMBER(38,0)"),
        Column("last_auth_approved", "NUMBER(38,0)"),
        Column("match_strength", "NUMBER(38,0)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)"),
        Column("statuscode", "NUMBER(38,0)"),
    ],
    watermark_column="datetime_modified",
)
