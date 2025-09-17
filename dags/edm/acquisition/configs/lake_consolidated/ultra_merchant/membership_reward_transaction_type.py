from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    table='membership_reward_transaction_type',
    column_list=[
        Column('membership_reward_transaction_type_id', 'INT', uniqueness=True),
        Column('type', 'VARCHAR(15)'),
        Column('label', 'VARCHAR(50)'),
        Column('object', 'VARCHAR(50)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('include_for_tier_calculation', 'INT'),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
    ],
    watermark_column='datetime_modified',
)
