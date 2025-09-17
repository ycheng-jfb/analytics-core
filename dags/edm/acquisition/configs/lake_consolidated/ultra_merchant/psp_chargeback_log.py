# todo payment_transaction_id should be added as key
from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='psp_chargeback_log',
    company_join_sql="""
    SELECT DISTINCT
        L.psp_chargeback_log_id,
        DS.company_id
    FROM {database}.REFERENCE.DIM_STORE AS DS
    INNER JOIN {database}.{schema}.CUSTOMER AS C
        ON C.STORE_GROUP_ID = DS.STORE_GROUP_ID
    INNER JOIN {database}.{schema}.PSP AS P
    ON  C.CUSTOMER_ID = P.CUSTOMER_ID
    INNER JOIN {database}.{schema}.PAYMENT_TRANSACTION_PSP AS PTP
    on  PTP.PSP_ID= P.PSP_ID
    INNER JOIN {database}.{source_schema}.psp_chargeback_log AS L
        ON PTP.PAYMENT_TRANSACTION_ID  = L.PAYMENT_TRANSACTION_ID """,
    column_list=[
        Column('psp_chargeback_log_id', 'INT', uniqueness=True, key=True),
        Column('payment_transaction_id', 'INT'),
        Column('response_transaction_id', 'VARCHAR(50)'),
        Column('merchant_id', 'VARCHAR(25)'),
        Column('transaction_type', 'VARCHAR(50)'),
        Column('transaction_method', 'VARCHAR(50)'),
        Column('payment_method', 'VARCHAR(25)'),
        Column('payment_amount', 'NUMBER(19, 4)'),
        Column('chargeback_amount', 'NUMBER(19, 4)'),
        Column('currency', 'VARCHAR(25)'),
        Column('reason_text', 'VARCHAR(50)'),
        Column('customer_email', 'VARCHAR(75)'),
        Column('customer_reference', 'VARCHAR(25)'),
        Column('customer_ip', 'VARCHAR(15)'),
        Column('customer_country', 'VARCHAR(2)'),
        Column('customer_risk', 'DOUBLE'),
        Column('issuer_country', 'VARCHAR(2)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('datetime_transaction', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_dispute', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_chargeback', 'TIMESTAMP_NTZ(3)'),
        Column('processed', 'BOOLEAN'),
    ],
    watermark_column='datetime_added',
)
