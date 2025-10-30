from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='membership_reward_transaction',
    watermark_column='datetime_modified',
    schema_version_prefix='v3',
    column_list=[
        Column('membership_reward_transaction_id', 'INT', uniqueness=True),
        Column('membership_id', 'INT'),
        Column('membership_reward_transaction_type_id', 'INT'),
        Column('membership_reward_plan_id', 'INT'),
        Column('administrator_id', 'INT'),
        Column('object_id', 'INT'),
        Column('points', 'INT'),
        Column('comment', 'VARCHAR(512)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('statuscode', 'INT'),
        Column('date_expires', 'TIMESTAMP_NTZ(0)'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
