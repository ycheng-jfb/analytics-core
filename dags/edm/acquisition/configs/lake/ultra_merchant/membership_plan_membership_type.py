from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="membership_plan_membership_type",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("membership_plan_membership_type_id", "INT", uniqueness=True),
        Column("membership_plan_id", "INT"),
        Column("membership_type_id", "INT"),
        Column("membership_product_id", "INT"),
        Column("price", "NUMBER(19, 4)"),
        Column("default_membership_reward_plan_id", "INT"),
        Column("default_shipping_option_id", "INT"),
        Column("max_prepaid_credits", "INT"),
        Column("membership_postreg_type_id", "INT"),
        Column("default_membership_invitation_incentive_id", "INT"),
        Column("default_personality_type_tag_id", "INT"),
        Column("default_billing_day", "INT"),
        Column("default_trial_period_days", "INT"),
        Column("active", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
