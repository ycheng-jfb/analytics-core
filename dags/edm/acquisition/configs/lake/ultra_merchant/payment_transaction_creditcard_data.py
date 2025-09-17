from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='payment_transaction_creditcard_data',
    watermark_column='datetime_modified',
    initial_load_value='2020-01-12',
    schema_version_prefix='v2',
    column_list=[
        Column('payment_transaction_id', 'INT', uniqueness=True),
        Column('creditcard_id', 'INT'),
        Column('funding_type', 'VARCHAR(25)'),
        Column('funding_balance', 'NUMBER(19, 4)'),
        Column('funding_reloadable', 'INT'),
        Column('is_prepaid', 'INT'),
        Column('prepaid_type', 'VARCHAR(25)'),
        Column('datetime_advised_retry', 'TIMESTAMP_NTZ(0)'),
        Column('card_product_type', 'VARCHAR(25)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
