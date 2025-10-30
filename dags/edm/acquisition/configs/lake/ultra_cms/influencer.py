from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultracms',
    schema='dbo',
    table='influencer',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('influencer_id', 'INT', uniqueness=True),
        Column('store_group_id', 'INT'),
        Column('customer_id', 'INT'),
        Column('membership_id', 'INT'),
        Column('firstname', 'VARCHAR(25)'),
        Column('lastname', 'VARCHAR(25)'),
        Column('email', 'VARCHAR(75)'),
        Column('type', 'VARCHAR(255)'),
        Column('description', 'VARCHAR(1024)'),
        Column('contract_url', 'VARCHAR(2048)'),
        Column('statuscode', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
