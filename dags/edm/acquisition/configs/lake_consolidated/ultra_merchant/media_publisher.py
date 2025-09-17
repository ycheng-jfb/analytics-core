from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    table='media_publisher',
    column_list=[
        Column('media_publisher_id', 'INT', uniqueness=True),
        Column('store_group_id', 'INT'),
        Column('label', 'VARCHAR(50)'),
        Column('enable_entry_popups', 'INT'),
        Column('enable_exit_popups', 'INT'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
    ],
    watermark_column='datetime_modified',
)
