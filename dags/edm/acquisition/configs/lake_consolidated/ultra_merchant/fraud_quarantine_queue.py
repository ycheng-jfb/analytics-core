from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="fraud_quarantine_queue",
    company_join_sql="""
       SELECT DISTINCT
           L.FRAUD_QUARANTINE_QUEUE_ID,
           DS.COMPANY_ID
       FROM {database}.REFERENCE.DIM_STORE AS DS
       INNER JOIN {database}.{schema}.CUSTOMER AS C
           ON C.STORE_GROUP_ID = DS.STORE_GROUP_ID
       INNER JOIN {database}.{source_schema}.fraud_quarantine_queue AS L
           ON L.CUSTOMER_ID = C.CUSTOMER_ID """,
    column_list=[
        Column("fraud_quarantine_queue_id", "INT", uniqueness=True, key=True),
        Column("customer_id", "INT", key=True),
        Column("administrator_id", "INT"),
        Column("datetime_assigned", "TIMESTAMP_NTZ(3)"),
        Column("datetime_closed", "TIMESTAMP_NTZ(3)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("statuscode", "INT"),
        Column("is_customer_verification_initiated", "BOOLEAN"),
        Column("hours_extended_for_verification", "INT"),
    ],
    watermark_column="datetime_modified",
)
