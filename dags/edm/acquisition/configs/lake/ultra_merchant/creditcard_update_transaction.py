from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='creditcard_update_transaction',
    watermark_column='datetime_modified',
    column_list=[
        Column('creditcard_update_transaction_id', 'INT', uniqueness=True),
        Column('creditcard_update_batch_id', 'INT'),
        Column('creditcard_id', 'INT'),
        Column('updated_creditcard_id', 'INT'),
        Column('response_transaction_id', 'VARCHAR(50)'),
        Column('response_result_code', 'VARCHAR(25)'),
        Column('response_result_text', 'VARCHAR(100)'),
        Column('comment', 'VARCHAR(255)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('statuscode', 'INT'),
    ],
)
