from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.OBJECT_COLUMN_NULL,
    table="modification_log",
    column_list=[
        Column("modification_log_id", "INT", uniqueness=True, key=True),
        Column("object", "VARCHAR(50)"),
        Column("object_id", "INT"),
        Column("field", "VARCHAR(50)"),
        Column("old_value", "VARCHAR"),
        Column("new_value", "VARCHAR"),
        Column("administrator_id", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_modified",
)
