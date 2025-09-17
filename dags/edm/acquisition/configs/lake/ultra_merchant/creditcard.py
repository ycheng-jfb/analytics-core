from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='creditcard',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('creditcard_id', 'INT', uniqueness=True),
        Column('customer_id', 'INT'),
        Column('address_id', 'INT'),
        Column('cnh', 'VARCHAR(32)'),
        Column('card_num', 'VARCHAR(255)'),
        Column('card_type', 'VARCHAR(15)'),
        Column('exp_month', 'VARCHAR(2)'),
        Column('exp_year', 'VARCHAR(2)'),
        Column('name_on_card', 'VARCHAR(50)'),
        Column('last_four_digits', 'VARCHAR(4)'),
        Column('method', 'VARCHAR(25)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_last_used', 'TIMESTAMP_NTZ(3)'),
        Column('statuscode', 'INT'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
