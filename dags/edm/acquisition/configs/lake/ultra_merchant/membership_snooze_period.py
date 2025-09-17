from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    schema_version_prefix="v3",
    table="membership_snooze_period",
    watermark_column="datetime_modified",
    column_list=[
        Column("membership_snooze_period_id", "INT", uniqueness=True),
        Column("membership_snooze_id", "INT"),
        Column("membership_id", "INT"),
        Column("period_id", "INT"),
        Column("membership_period_id", "INT"),
        Column("membership_billing_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
