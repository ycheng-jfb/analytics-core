from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='customer_order_verification',
    company_join_sql="""
        SELECT DISTINCT
            L.CUSTOMER_ORDER_VERIFICATION_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        LEFT JOIN {database}.{source_schema}."ORDER" AS O
            ON DS.STORE_ID = O.STORE_ID
        LEFT JOIN {database}.{source_schema}.customer_order_verification AS L
            ON O.ORDER_ID = L.ORDER_ID""",
    column_list=[
        Column('customer_order_verification_id', 'INT', uniqueness=True, key=True),
        Column('fraud_quarantine_queue_id', 'INT', key=True),
        Column('order_id', 'INT', key=True),
        Column('statuscode', 'INT'),
        Column('administrator_id', 'INT', key=True),
        Column('is_auto_verify', 'BOOLEAN'),
        Column('datetime_expires', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)'),
    ],
    watermark_column='datetime_modified',
)
