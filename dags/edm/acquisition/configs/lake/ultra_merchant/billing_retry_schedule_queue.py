from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='billing_retry_schedule_queue',
    watermark_column='datetime_modified',
    initial_load_value='2020-01-14',
    schema_version_prefix='v3',
    column_list=[
        Column('billing_retry_schedule_queue_id', 'INT', uniqueness=True),
        Column('billing_retry_queue_id', 'INT'),
        Column('billing_retry_schedule_id', 'INT'),
        Column('order_id', 'INT'),
        Column('order_type_id', 'INT'),
        Column('payment_number', 'INT'),
        Column('last_cycle', 'INT'),
        Column('last_phone_cycle', 'INT'),
        Column('last_billing_retry_schedule_queue_contact_log_id', 'INT'),
        Column('contact_am_pm', 'VARCHAR(2)'),
        Column('store_group_id', 'INT'),
        Column('administrator_id', 'INT'),
        Column('completed_by', 'VARCHAR(25)'),
        Column('datetime_initial_decline', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_scheduled', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_last_retry', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_assigned', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('statuscode', 'INT'),
        Column('billing_retry_schedule_type_id', 'INT'),
    ],
)
