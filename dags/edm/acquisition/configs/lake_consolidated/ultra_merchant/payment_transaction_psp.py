from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='payment_transaction_psp',
    company_join_sql="""
         SELECT DISTINCT
             L.PAYMENT_TRANSACTION_ID,
             DS.COMPANY_ID
         FROM {database}.REFERENCE.DIM_STORE AS DS
         INNER JOIN {database}.{schema}."ORDER" AS O
         ON DS.STORE_ID = O.STORE_ID
          INNER JOIN {database}.{source_schema}.payment_transaction_psp  L
           ON O.ORDER_ID = L.ORDER_ID """,
    column_list=[
        Column('payment_transaction_id', 'INT', uniqueness=True, key=True),
        Column('order_id', 'INT', key=True),
        Column('psp_id', 'INT', key=True),
        Column('address_id', 'INT', key=True),
        Column('gateway_id', 'INT'),
        Column('gateway_account_id', 'INT'),
        Column('session_id', 'INT', key=True),
        Column('original_payment_transaction_id', 'INT'),
        Column('transaction_type', 'VARCHAR(25)'),
        Column('gateway_name', 'VARCHAR(25)'),
        Column('gateway_account', 'VARCHAR(50)'),
        Column('gateway_api_version', 'VARCHAR(10)'),
        Column('amount', 'NUMBER(19, 4)'),
        Column('request_transaction_id', 'VARCHAR(50)'),
        Column('request_auth_code', 'VARCHAR(50)'),
        Column('response_transaction_id', 'VARCHAR(50)'),
        Column('response_auth_code', 'VARCHAR(50)'),
        Column('response_result_code', 'VARCHAR(25)'),
        Column('response_result_subcode', 'VARCHAR(25)'),
        Column('response_result_text', 'VARCHAR(50)'),
        Column('response_payment_method', 'VARCHAR(50)'),
        Column('response_avs_code', 'VARCHAR(25)'),
        Column('response_card_code', 'VARCHAR(25)'),
        Column('response_cavv', 'VARCHAR(25)'),
        Column('response_reason_code', 'VARCHAR(50)'),
        Column('response_reason_text', 'VARCHAR(255)'),
        Column('response_latency', 'INT'),
        Column('response_data_1', 'VARCHAR(1000)'),
        Column('response_data_2', 'VARCHAR(1000)'),
        Column('response_data_3', 'VARCHAR(255)'),
        Column('ip', 'VARCHAR(15)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('statuscode', 'INT'),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
    ],
    watermark_column='datetime_modified',
)
