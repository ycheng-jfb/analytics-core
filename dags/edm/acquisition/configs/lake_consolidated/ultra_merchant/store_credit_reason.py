from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    table='store_credit_reason',
    column_list=[
        Column('store_credit_reason_id', 'INT', uniqueness=True),
        Column('label', 'VARCHAR(50)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('cash', 'INT'),
        Column('access', 'VARCHAR(15)'),
    ],
    watermark_column='datetime_added',
)
