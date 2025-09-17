from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="membership_reward_tier",
    schema_version_prefix="v2",
    column_list=[
        Column("membership_reward_tier_id", "INT", uniqueness=True),
        Column("membership_reward_plan_id", "INT"),
        Column("membership_level_group_id", "INT"),
        Column("label", "VARCHAR(50)"),
        Column("required_points_earned", "INT"),
    ],
)
