from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.OBJECT_COLUMN_NULL,
    table="administrator_session_log",
    column_list=[
        Column("administrator_session_log_id", "INT", uniqueness=True, key=True),
        Column("administrator_session_id", "INT", key=True),
        Column("object", "VARCHAR(50)"),
        Column("object_id", "INT"),
        Column("comment", "VARCHAR(255)"),
        Column("query_string", "VARCHAR(255)"),
        Column("ip", "VARCHAR(15)"),
        Column("data_1", "VARCHAR(255)"),
        Column("data_2", "VARCHAR(255)"),
        Column("data_3", "VARCHAR(255)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_added",
)
