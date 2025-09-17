from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="membership_level",
    watermark_column="datetime_added",
    schema_version_prefix="v2",
    column_list=[
        Column("membership_level_id", "INT", uniqueness=True),
        Column("membership_plan_id", "INT"),
        Column("membership_level_event_id", "INT"),
        Column("membership_level_group_id", "INT"),
        Column("email_template_type_id", "INT"),
        Column("level", "INT"),
        Column("label", "VARCHAR(100)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
