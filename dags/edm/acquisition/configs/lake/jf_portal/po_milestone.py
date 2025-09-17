from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="jf_portal",
    schema="dbo",
    table="PoMilestone",
    is_full_upsert=True,
    enable_archive=True,
    schema_version_prefix="v4",
    column_list=[
        Column("po_milestone_id", "INT", uniqueness=True, source_name="PoMilestoneId"),
        Column("milestone_event_id", "INT", source_name="MilestoneEventId"),
        Column("po_id", "INT", source_name="PoId"),
        Column("name", "VARCHAR(50)", source_name="Name"),
        Column('"ORDER"', "INT", source_name="[Order]"),
        Column("date_est", "DATE", source_name="DateEst"),
        Column("date_actual", "DATE", source_name="DateActual"),
        Column("date_sched", "DATE", source_name="DateSched"),
        Column("date_complete", "DATE", source_name="DateComplete"),
        Column("user_create", "VARCHAR(100)", source_name="UserCreate"),
        Column(
            "date_create", "TIMESTAMP_NTZ(3)", delta_column=1, source_name="DateCreate"
        ),
        Column("user_update", "VARCHAR(100)", source_name="UserUpdate"),
        Column(
            "date_update", "TIMESTAMP_NTZ(3)", delta_column=0, source_name="DateUpdate"
        ),
    ],
)
