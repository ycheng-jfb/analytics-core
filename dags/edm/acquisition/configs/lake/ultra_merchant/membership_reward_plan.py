from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='membership_reward_plan',
    watermark_column='datetime_modified',
    strict_inequality=True,
    schema_version_prefix='v2',
    column_list=[
        Column('membership_reward_plan_id', 'INT', uniqueness=True),
        Column('membership_plan_id', 'INT'),
        Column('label', 'VARCHAR(50)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('purchase_point_multiplier', 'DOUBLE'),
        Column('default_reward_credit_redemption_type', 'VARCHAR(50)'),
        Column('eligible_membership_level_group_ids', 'VARCHAR(50)'),
        Column('membership_cancellation_forfeits_points', 'BOOLEAN'),
        Column('tier_calculation_months', 'INT'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
