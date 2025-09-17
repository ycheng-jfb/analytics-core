from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    table='carrier_uw',
    column_list=[
        Column('carrier_uw_id', 'INT', uniqueness=True),
        Column(
            'carrier_id',
            'INT',
        ),
        Column('label', 'VARCHAR(255)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('tracking_url', 'VARCHAR(255)'),
        Column('carrier_hash', 'BINARY(20)'),
    ],
    watermark_column='datetime_modified',
)
