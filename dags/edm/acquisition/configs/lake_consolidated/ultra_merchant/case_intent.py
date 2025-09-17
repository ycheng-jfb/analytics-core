from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    table="case_intent",
    column_list=[
        Column("case_intent_id", "INT", uniqueness=True),
        Column("case_intent_code", "VARCHAR(255)"),
        Column("label", "VARCHAR(255)"),
        Column("object", "VARCHAR(255)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("rate_limit", "INT"),
    ],
    watermark_column="datetime_modified",
)
