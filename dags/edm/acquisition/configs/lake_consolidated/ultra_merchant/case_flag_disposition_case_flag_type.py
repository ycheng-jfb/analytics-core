from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    table="case_flag_disposition_case_flag_type",
    column_list=[
        Column("case_flag_disposition_id", "INT", uniqueness=True),
        Column("case_flag_type_id", "INT", uniqueness=True),
        Column("sort_order", "INT"),
        Column("form_control_type", "VARCHAR(50)"),
        Column("is_required", "INT"),
        Column("unique_identifier", "VARCHAR(50)"),
    ],
)
