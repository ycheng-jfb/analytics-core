from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='chargeback_import_transaction',
    company_join_sql="""
     SELECT DISTINCT
         L.CHARGEBACK_IMPORT_TRANSACTION_ID,
         DS.COMPANY_ID
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{schema}."ORDER" AS O
     ON DS.STORE_ID = O.STORE_ID
     INNER JOIN {database}.{source_schema}.chargeback_import_transaction AS L
     ON L.ORDER_ID=O.ORDER_ID """,
    column_list=[
        Column('chargeback_import_transaction_id', 'INT', uniqueness=True, key=True),
        Column('chargeback_import_batch_detail_id', 'INT', key=True),
        Column('customer_id', 'INT', key=True),
        Column('order_id', 'INT', key=True),
        Column('payment_transaction_id', 'INT'),
        Column('is_duplicate', 'BOOLEAN'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('statuscode', 'VARCHAR(30)'),
    ],
    watermark_column='datetime_added',
)
