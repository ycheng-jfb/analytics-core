from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='chargeback_import_batch_detail',
    company_join_sql="""
            SELECT DISTINCT l.chargeback_import_batch_detail_id, ds.company_id
            FROM {database}.{schema}.chargeback_import_batch_detail l
                 JOIN {database}.{schema}.payment_transaction_creditcard p
                      ON p.payment_transaction_id = l.payment_transaction_id
                      AND l.payment_transaction_id <> ''
                 JOIN {database}.{schema}."ORDER" o
                      ON o.order_id = p.order_id
                 JOIN {database}.reference.dim_store ds
                      ON o.store_id = ds.store_id""",
    column_list=[
        Column('chargeback_import_batch_detail_id', 'INT', uniqueness=True, key=True),
        Column('chargeback_import_batch_id', 'INT', key=True),
        Column('gateway_account_id', 'INT'),
        Column('payment_transaction_id', 'VARCHAR(15)', key=True),
        Column('foreign_case_id', 'VARCHAR(30)', key=True),
        Column('first_four_digits', 'VARCHAR(4)'),
        Column('last_four_digits', 'VARCHAR(4)'),
        Column('amount', 'NUMBER(19, 4)'),
        Column('status', 'VARCHAR(64)'),
        Column('reason', 'VARCHAR(1000)'),
        Column('datetime_transacted', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_notified', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_posted', 'TIMESTAMP_NTZ(3)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
    ],
    watermark_column='datetime_added',
)
