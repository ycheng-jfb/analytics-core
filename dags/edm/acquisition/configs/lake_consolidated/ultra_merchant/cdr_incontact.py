from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="cdr_incontact",
    column_list=[
        Column("cdr_incontact_id", "INT", uniqueness=True),
        Column("contact_id", "INT"),
        Column("master_contact_id", "INT"),
        Column("ani", "VARCHAR(255)"),
        Column("contact_name", "VARCHAR(255)"),
        Column("media_name", "VARCHAR(255)"),
        Column("skill_id", "INT"),
        Column("skill_name", "VARCHAR(255)"),
        Column("campaign_id", "INT"),
        Column("campaign_name", "VARCHAR(255)"),
        Column("agent_id", "INT"),
        Column("agent_name", "VARCHAR(255)"),
        Column("team_id", "INT"),
        Column("team_name", "VARCHAR(255)"),
        Column("datetime_start", "TIMESTAMP_NTZ(3)"),
        Column("seconds_sla", "INT"),
        Column("seconds_pre_queue", "INT"),
        Column("seconds_in_queue", "INT"),
        Column("seconds_post_queue", "INT"),
        Column("seconds_agent", "INT"),
        Column("seconds_abandon", "INT"),
        Column("seconds_total", "INT"),
        Column("did_abandon", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("seconds_hold", "INT"),
    ],
    watermark_column="datetime_modified",
)
