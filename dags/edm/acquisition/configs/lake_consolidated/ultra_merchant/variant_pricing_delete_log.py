from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='variant_pricing_delete_log',
    column_list=[
        Column('variant_pricing_delete_log_id', 'INT'),
        Column('variant_pricing_id', 'INT', uniqueness=True),
        Column('test_metadata_id', 'INT'),
        Column('product_id', 'INT'),
        Column('pricing_id', 'INT'),
        Column('control_pricing_id', 'INT'),
        Column('variant_number', 'INT'),
        Column('statuscode', 'INT'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('datetime_deleted', 'TIMESTAMP_NTZ(3)'),
    ],
    watermark_column='datetime_modified',
)
