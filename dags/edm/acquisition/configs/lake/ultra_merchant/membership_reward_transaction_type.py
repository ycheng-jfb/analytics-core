from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="membership_reward_transaction_type",
    watermark_column="datetime_modified",
    strict_inequality=True,
    schema_version_prefix="v2",
    column_list=[
        Column("membership_reward_transaction_type_id", "INT", uniqueness=True),
        Column("type", "VARCHAR(15)"),
        Column("label", "VARCHAR(50)"),
        Column("object", "VARCHAR(50)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("include_for_tier_calculation", "INT"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
