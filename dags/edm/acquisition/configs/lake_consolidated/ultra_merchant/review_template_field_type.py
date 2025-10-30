from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    table='review_template_field_type',
    column_list=[
        Column('review_template_field_type_id', 'INT', uniqueness=True),
        Column('label', 'VARCHAR(100)'),
    ],
)
