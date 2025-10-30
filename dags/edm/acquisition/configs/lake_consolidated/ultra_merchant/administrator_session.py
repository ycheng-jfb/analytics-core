from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='administrator_session',
    column_list=[
        Column('administrator_session_id', 'INT', uniqueness=True),
        Column('administrator_id', 'INT'),
        Column('session_key', 'VARCHAR(36)'),
        Column('ip', 'VARCHAR(15)'),
        Column('date_added', 'TIMESTAMP_NTZ(0)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('statuscode', 'INT'),
    ],
    watermark_column='datetime_modified',
)
