from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='psp_notification_log',
    company_join_sql="""
        SELECT DISTINCT
            L.PSP_NOTIFICATION_LOG_ID,
            COALESCE(ds.company_id, ods.company_id,
                CASE WHEN l.merchant_id ILIKE 'JustFab%' THEN 10
                     WHEN l.merchant_id ILIKE 'Fabletics%' OR l.merchant_id ILIKE 'FL%' THEN 20
                     WHEN l.merchant_id ILIKE 'Savage%' THEN 30
                    END) AS company_id
        FROM {database}.{source_schema}.psp_notification_log L
        LEFT JOIN {database}.{schema}.PAYMENT_TRANSACTION_PSP PTP
            ON L.PAYMENT_TRANSACTION_ID = PTP.PAYMENT_TRANSACTION_ID
        LEFT JOIN {database}.{schema}.PSP P
            ON PTP.PSP_ID = P.PSP_ID
        LEFT JOIN {database}.{schema}.CUSTOMER C
            ON P.CUSTOMER_ID = C.CUSTOMER_ID
        LEFT JOIN {database}.REFERENCE.DIM_STORE DS
            ON DS.STORE_GROUP_ID = C.STORE_GROUP_ID
        LEFT JOIN {database}.{source_schema}."ORDER" o
            ON ptp.order_id = o.order_id
        LEFT JOIN {database}.reference.dim_store ods
            ON o.store_id = ods.store_id""",
    column_list=[
        Column('psp_notification_log_id', 'INT', uniqueness=True, key=True),
        Column('payment_transaction_id', 'INT'),
        Column('request_transaction_id', 'VARCHAR(50)'),
        Column('response_transaction_id', 'VARCHAR(50)'),
        Column('merchant_id', 'VARCHAR(25)'),
        Column('transaction_type', 'VARCHAR(50)'),
        Column('payment_method', 'VARCHAR(25)'),
        Column('amount', 'NUMBER(19, 4)'),
        Column('currency', 'VARCHAR(25)'),
        Column('reason_text', 'VARCHAR(255)'),
        Column('supported_operations', 'VARCHAR(50)'),
        Column('environment', 'VARCHAR(25)'),
        Column('success', 'VARCHAR(15)'),
        Column('ip', 'VARCHAR(15)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('datetime_transaction', 'TIMESTAMP_NTZ(3)'),
        Column('processed', 'INT'),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('psp_notification_log_detail_id', 'INT'),
    ],
    watermark_column='datetime_modified',
)
