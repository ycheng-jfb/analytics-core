from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='rma',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('rma_id', 'INT', uniqueness=True),
        Column('order_id', 'INT'),
        Column('rma_source_id', 'INT'),
        Column('administrator_id', 'INT'),
        Column('return_action_request_id', 'INT'),
        Column('return_reason_id', 'INT'),
        Column('reason_comment', 'VARCHAR(255)'),
        Column('contains_unidentified_items', 'INT'),
        Column('return_days', 'INT'),
        Column('suppress_email', 'INT'),
        Column('product_refund', 'NUMBER(19, 4)'),
        Column('tax_refund', 'NUMBER(19, 4)'),
        Column('shipping_refund', 'NUMBER(19, 4)'),
        Column('billing_refund', 'NUMBER(19, 4)'),
        Column('date_added', 'TIMESTAMP_NTZ(0)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_resolved', 'TIMESTAMP_NTZ(3)'),
        Column('date_return_due', 'TIMESTAMP_NTZ(3)'),
        Column('date_expires', 'TIMESTAMP_NTZ(3)'),
        Column('statuscode', 'INT'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('datetime_pickup_expected', 'TIMESTAMP_NTZ(3)'),
    ],
)
