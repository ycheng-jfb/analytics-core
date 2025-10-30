from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='membership_token_transaction',
    schema_version_prefix='v3',
    watermark_column='datetime_modified',
    column_list=[
        Column('membership_token_transaction_id', 'INT', uniqueness=True),
        Column('membership_token_id', 'INT'),
        Column('membership_token_transaction_type_id', 'INT'),
        Column('membership_token_transaction_reason_id', 'INT'),
        Column('administrator_id', 'INT'),
        Column('object', 'VARCHAR(50)'),
        Column('object_id', 'INT'),
        Column('amount', 'NUMBER(19, 4)'),
        Column('comment', 'VARCHAR(512)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('datetime_transaction', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_local_transaction', 'TIMESTAMP_NTZ(3)'),
        Column('cancelled', 'INT'),
    ],
)
