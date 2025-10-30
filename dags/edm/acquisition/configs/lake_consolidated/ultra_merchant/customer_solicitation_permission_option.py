from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    table='customer_solicitation_permission_option',
    column_list=[
        Column('customer_solicitation_permission_option_id', 'INT', uniqueness=True),
        Column('customer_solicitation_permission_option_type_id', 'INT'),
        Column('label', 'VARCHAR(100)'),
        Column('description', 'VARCHAR(250)'),
        Column('value', 'INT'),
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
