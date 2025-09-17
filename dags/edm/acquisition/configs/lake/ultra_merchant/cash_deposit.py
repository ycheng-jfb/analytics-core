from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='cash_deposit',
    watermark_column='datetime_modified',
    strict_inequality=True,
    schema_version_prefix='v2',
    column_list=[
        Column('cash_deposit_id', 'INT', uniqueness=True),
        Column('store_id', 'INT'),
        Column('amount', 'NUMBER(19, 4)'),
        Column('administrator_id', 'INT'),
        Column('deposited_administrator_id', 'INT'),
        Column('deposit_image', 'VARCHAR(255)'),
        Column('statuscode', 'INT'),
        Column('date_deposit', 'DATE'),
        Column('datetime_deposited', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
