from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
    TableType,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="bond_user_action_type",
    table_type=TableType.NSYNC,
    column_list=[
        Column("bond_user_action_type_id", "NUMBER", uniqueness=True),
        Column("label", "VARCHAR(255)"),
        Column("is_action_visible_to_administrator", "BOOLEAN"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)"),
    ],
    watermark_column="datetime_modified",
)
