from include.airflow.operators.mssql_acquisition import HighWatermarkMaxRowVersion
from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='um_replicated',
    schema='dbo',
    table='ultramerchant__refund__del',
    watermark_column='repl_timestamp',
    initial_load_value='0x0',
    high_watermark_cls=HighWatermarkMaxRowVersion,
    schema_version_prefix='v2',
    column_list=[
        Column('refund_id', 'INT'),
        Column('order_id', 'INT'),
        Column('payment_id', 'INT'),
        Column('administrator_id', 'INT'),
        Column('payment_method', 'VARCHAR(25)'),
        Column('payment_transaction_id', 'INT'),
        Column('refund_reason_id', 'INT'),
        Column('reason_comment', 'VARCHAR(255)'),
        Column('product_refund', 'NUMBER(19, 4)'),
        Column('shipping_refund', 'NUMBER(19, 4)'),
        Column('tax_refund', 'NUMBER(19, 4)'),
        Column('total_refund', 'NUMBER(19, 4)'),
        Column('approved_administrator_id', 'INT'),
        Column('date_added', 'TIMESTAMP_NTZ(0)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('statuscode', 'INT'),
        Column('datetime_refunded', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_transaction', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_local_transaction', 'TIMESTAMP_NTZ(3)'),
        Column('repl_action', 'VARCHAR(1)'),
        Column('repl_timestamp', 'BINARY(8)', uniqueness=True),
    ],
)
