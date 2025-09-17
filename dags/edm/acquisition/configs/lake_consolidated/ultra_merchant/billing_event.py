from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.REGULAR,
    table="billing_event",
    company_join_sql="""
            SELECT DISTINCT
                l.billing_event_id,
                DS.company_id
            FROM {database}.REFERENCE.DIM_STORE AS DS
            INNER JOIN {database}.{schema}."ORDER" AS O
                ON DS.STORE_ID = O.STORE_ID
            INNER JOIN {database}.{source_schema}.billing_event AS l
                ON l.order_id = O.order_id""",
    column_list=[
        Column("billing_event_id", "INT", uniqueness=True, key=True),
        Column("billing_processor_id", "INT"),
        Column("billing_event_type_id", "INT"),
        Column("order_id", "INT", key=True),
        Column("membership_id", "INT", key=True),
        Column("retry_billing_processor_id", "INT"),
        Column("refund_id", "INT", key=True),
        Column("foreign_event_id", "VARCHAR(50)"),
        Column("payment_method", "VARCHAR(25)"),
        Column("retry_cycle", "INT"),
        Column("retry_active", "INT"),
        Column("consecutive_failed_billing_periods", "INT"),
        Column("last_billed_period_id", "INT"),
        Column("error_message", "VARCHAR(255)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_billing_failed_since",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("success", "INT"),
        Column("statuscode", "INT"),
    ],
    watermark_column="datetime_modified",
)
