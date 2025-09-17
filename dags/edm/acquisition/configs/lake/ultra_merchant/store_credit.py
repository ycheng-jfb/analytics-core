from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='store_credit',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('store_credit_id', 'INT', uniqueness=True),
        Column('customer_id', 'INT'),
        Column('administrator_id', 'INT'),
        Column('store_credit_reason_id', 'INT'),
        Column('reason_comment', 'VARCHAR(255)'),
        Column('code', 'VARCHAR(25)'),
        Column('amount', 'NUMBER(19, 4)'),
        Column('balance', 'NUMBER(19, 4)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('date_expires', 'TIMESTAMP_NTZ(0)'),
        Column('statuscode', 'INT'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('currency_code', 'VARCHAR(3)'),
    ],
)
