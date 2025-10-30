from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='creditcard_update_batch',
    watermark_column='datetime_added',
    column_list=[
        Column('creditcard_update_batch_id', 'INT', uniqueness=True),
        Column('gateway_account_id', 'INT'),
        Column('comment', 'VARCHAR(255)'),
        Column('total_creditcards', 'INT'),
        Column('updated_creditcards', 'INT'),
        Column('foreign_batch_id', 'VARCHAR(50)'),
        Column('foreign_session_id', 'VARCHAR(50)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('date_next_billing_start', 'TIMESTAMP_NTZ(0)'),
        Column('date_next_billing_end', 'TIMESTAMP_NTZ(0)'),
        Column('date_posted', 'TIMESTAMP_NTZ(0)'),
        Column('date_results_expected', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_touched', 'TIMESTAMP_NTZ(3)'),
        Column('statuscode', 'INT'),
    ],
)
