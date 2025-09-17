from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='exchange',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('exchange_id', 'INT', uniqueness=True),
        Column('rma_id', 'INT'),
        Column('original_order_id', 'INT'),
        Column('exchange_order_id', 'INT'),
        Column('session_id', 'INT'),
        Column('refund_return_action_id', 'INT'),
        Column('charge_shipping', 'INT'),
        Column('charge_return_shipping', 'INT'),
        Column('date_added', 'TIMESTAMP_NTZ(0)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('statuscode', 'INT'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
