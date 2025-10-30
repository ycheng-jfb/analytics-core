from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='payment',
    company_join_sql="""
       SELECT DISTINCT
           L.PAYMENT_ID,
           DS.COMPANY_ID
       FROM {database}.REFERENCE.DIM_STORE AS DS
       INNER JOIN {database}.{schema}."ORDER" AS C
           ON DS.STORE_ID = C.STORE_ID
       INNER JOIN {database}.{source_schema}.payment AS L
           ON L.ORDER_ID = C.ORDER_ID """,
    column_list=[
        Column('payment_id', 'INT', uniqueness=True, key=True),
        Column('order_id', 'INT', key=True),
        Column('payment_method', 'VARCHAR(25)'),
        Column('payment_object_id', 'INT'),
        Column('auth_payment_transaction_id', 'INT', key=True),
        Column('capture_payment_transaction_id', 'INT', key=True),
        Column('payment_number', 'INT'),
        Column('amount', 'NUMBER(19, 4)'),
        Column('original_amount', 'NUMBER(19, 4)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('date_payment_originally_due', 'TIMESTAMP_NTZ(0)'),
        Column('date_payment_due', 'TIMESTAMP_NTZ(0)'),
        Column('statuscode', 'INT'),
        Column('datetime_transaction', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_local_transaction', 'TIMESTAMP_NTZ(3)'),
        Column('subtotal', 'NUMBER(19, 4)'),
        Column('shipping', 'NUMBER(19, 4)'),
        Column('tax', 'NUMBER(19, 4)'),
    ],
    watermark_column='datetime_modified',
)
