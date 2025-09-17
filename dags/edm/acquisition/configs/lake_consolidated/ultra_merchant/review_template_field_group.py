from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table="review_template_field_group",
    table_type=TableType.NSYNC,
    column_list=[
        Column("review_template_field_group_id", "INT", uniqueness=True, key=True),
        Column("label", "VARCHAR(100)"),
        Column("aggregate_fields", "INT"),
        Column("aggregate_by_answers", "INT"),
    ],
)
