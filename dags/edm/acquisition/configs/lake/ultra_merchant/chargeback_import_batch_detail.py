from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='chargeback_import_batch_detail',
    watermark_column='datetime_added',
    schema_version_prefix='v2',
    column_list=[
        Column('chargeback_import_batch_detail_id', 'INT', uniqueness=True),
        Column('chargeback_import_batch_id', 'INT'),
        Column('gateway_account_id', 'INT'),
        Column('payment_transaction_id', 'VARCHAR(15)'),
        Column('foreign_case_id', 'VARCHAR(30)'),
        Column('first_four_digits', 'VARCHAR(4)'),
        Column('last_four_digits', 'VARCHAR(4)'),
        Column('amount', 'NUMBER(19, 4)'),
        Column('status', 'VARCHAR(64)'),
        Column('reason', 'VARCHAR(1000)'),
        Column('datetime_transacted', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_notified', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_posted', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
