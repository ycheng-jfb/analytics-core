from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='credit_transfer_transaction_type',
    watermark_column='datetime_added',
    schema_version_prefix='v2',
    column_list=[
        Column('credit_transfer_transaction_type_id', 'INT', uniqueness=True),
        Column('type', 'VARCHAR(25)'),
        Column('label', 'VARCHAR(100)'),
        Column('object', 'VARCHAR(50)'),
        Column('foreign_source_code', 'VARCHAR(50)'),
        Column('debit_credit_transfer_transaction_type_id', 'INT'),
        Column('store_credit_reason_id', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
