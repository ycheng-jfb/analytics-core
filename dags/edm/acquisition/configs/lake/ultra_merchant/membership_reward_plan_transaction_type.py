from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='membership_reward_plan_transaction_type',
    watermark_column='datetime_added',
    schema_version_prefix='v2',
    column_list=[
        Column('membership_reward_plan_transaction_type_id', 'INT', uniqueness=True),
        Column('membership_reward_plan_id', 'INT'),
        Column('membership_reward_transaction_type_id', 'INT'),
        Column('description', 'VARCHAR(2000)'),
        Column('points', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
