from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='membership_reward_signal',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('membership_reward_signal_id', 'INT', uniqueness=True),
        Column('membership_id', 'INT'),
        Column('membership_reward_tier_id', 'INT'),
        Column('membership_level_id', 'INT'),
        Column('membership_tier_points', 'INT'),
        Column('points_to_next_tier', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
