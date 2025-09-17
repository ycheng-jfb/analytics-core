from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='customer_foreign_account',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('customer_foreign_account_id', 'INT', uniqueness=True),
        Column('customer_foreign_account_type_id', 'INT'),
        Column('customer_id', 'INT'),
        Column('foreign_account_id', 'VARCHAR(50)'),
        Column('datetime_relationship_updated', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('active', 'INT'),
    ],
)
