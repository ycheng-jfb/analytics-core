from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="billing_retry_schedule_queue_log",
    company_join_sql="""
       SELECT DISTINCT
           L.billing_retry_schedule_queue_log_id,
           DS.COMPANY_ID
       FROM {database}.REFERENCE.DIM_STORE AS DS
       INNER JOIN {database}.{schema}."ORDER" AS O
           ON DS.STORE_ID = O.STORE_ID
       INNER JOIN {database}.{schema}.billing_retry_schedule_queue AS LRSQ
           ON LRSQ.ORDER_ID = O.ORDER_ID
       INNER JOIN {database}.{source_schema}.billing_retry_schedule_queue_log AS L
       ON LRSQ.billing_retry_schedule_queue_id=L.billing_retry_schedule_queue_id """,
    column_list=[
        Column("billing_retry_schedule_queue_log_id", "INT", uniqueness=True, key=True),
        Column("billing_retry_schedule_queue_id", "INT", key=True),
        Column("cycle", "INT"),
        Column("payment_method", "VARCHAR(25)"),
        Column("auth_payment_transaction_id", "INT", key=True),
        Column("datetime_added", "TIMESTAMP_NTZ(3)"),
    ],
    watermark_column="billing_retry_schedule_queue_log_id",
)
