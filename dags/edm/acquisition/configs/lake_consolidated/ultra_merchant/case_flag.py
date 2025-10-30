from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    table='case_flag',
    column_list=[
        Column('case_flag_id', 'INT', uniqueness=True),
        Column('parent_case_flag_id', 'INT'),
        Column('case_flag_type_id', 'INT'),
        Column('label', 'VARCHAR(250)'),
        Column('instruction_html', 'VARCHAR'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('sort', 'INT'),
        Column('statuscode', 'INT'),
    ],
    watermark_column='datetime_modified',
)
