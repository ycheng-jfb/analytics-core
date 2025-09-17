from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    table="membership_level",
    column_list=[
        Column("membership_level_id", "INT", uniqueness=True),
        Column("membership_plan_id", "INT"),
        Column("membership_level_event_id", "INT"),
        Column("membership_level_group_id", "INT"),
        Column("email_template_type_id", "INT"),
        Column("level", "INT"),
        Column("label", "VARCHAR(100)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_added",
)
