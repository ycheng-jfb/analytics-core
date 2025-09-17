from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='address_validation_request_log',
    watermark_column='datetime_added',
    schema_version_prefix='v2',
    column_list=[
        Column('address_validation_request_log_id', 'INT', uniqueness=True),
        Column('customer_id', 'INT'),
        Column('session_id', 'INT'),
        Column('latency', 'INT'),
        Column('address1', 'VARCHAR(50)'),
        Column('address2', 'VARCHAR(25)'),
        Column('city', 'VARCHAR(35)'),
        Column('state', 'VARCHAR(25)'),
        Column('zip', 'VARCHAR(25)'),
        Column('statuscode', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
