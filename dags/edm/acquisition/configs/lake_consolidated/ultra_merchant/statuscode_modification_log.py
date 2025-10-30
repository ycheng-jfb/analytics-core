from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.OBJECT_COLUMN,
    table='statuscode_modification_log',
    column_list=[
        Column('statuscode_modification_log_id', 'INT', uniqueness=True),
        Column('object', 'VARCHAR(50)'),
        Column('object_id', 'INT'),
        Column('from_statuscode', 'INT'),
        Column('to_statuscode', 'INT'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
    ],
    watermark_column='datetime_added',
)
