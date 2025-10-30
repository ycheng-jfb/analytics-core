from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='media_code',
    column_list=[
        Column('media_code_id', 'INT', uniqueness=True, key=True),
        Column('media_code_type_id', 'INT'),
        Column('media_type_id', 'INT'),
        Column('media_publisher_id', 'INT'),
        Column('administrator_id', 'INT'),
        Column('code', 'VARCHAR(255)'),
        Column('label', 'VARCHAR(255)'),
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
