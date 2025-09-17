from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="billing_retry_schedule_queue",
    company_join_sql="""
       SELECT DISTINCT
           L.BILLING_RETRY_SCHEDULE_QUEUE_ID,
           DS.COMPANY_ID
       FROM {database}.REFERENCE.DIM_STORE AS DS
       INNER JOIN {database}.{schema}."ORDER" AS C
           ON DS.STORE_ID = C.STORE_ID
       INNER JOIN {database}.{source_schema}.billing_retry_schedule_queue AS L
           ON L.ORDER_ID = C.ORDER_ID """,
    column_list=[
        Column("billing_retry_schedule_queue_id", "INT", uniqueness=True, key=True),
        Column("billing_retry_queue_id", "INT"),
        Column("billing_retry_schedule_id", "INT"),
        Column("order_id", "INT", key=True),
        Column("order_type_id", "INT"),
        Column("payment_number", "INT"),
        Column("last_cycle", "INT"),
        Column("last_phone_cycle", "INT"),
        Column("last_billing_retry_schedule_queue_contact_log_id", "INT"),
        Column("contact_am_pm", "VARCHAR(2)"),
        Column("store_group_id", "INT"),
        Column("administrator_id", "INT"),
        Column("completed_by", "VARCHAR(25)"),
        Column("datetime_initial_decline", "TIMESTAMP_NTZ(3)"),
        Column("datetime_scheduled", "TIMESTAMP_NTZ(3)"),
        Column("datetime_last_retry", "TIMESTAMP_NTZ(3)"),
        Column("datetime_assigned", "TIMESTAMP_NTZ(3)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("statuscode", "INT"),
        Column("billing_retry_schedule_type_id", "INT"),
    ],
    watermark_column="datetime_modified",
)
