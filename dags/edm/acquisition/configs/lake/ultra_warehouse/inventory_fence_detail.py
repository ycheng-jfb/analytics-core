from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultrawarehouse',
    schema='dbo',
    table='inventory_fence_detail',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('inventory_fence_detail_id', 'INT', uniqueness=True),
        Column('inventory_fence_id', 'INT'),
        Column('fence_type_id', 'INT'),
        Column('region_id', 'INT'),
        Column('item_id', 'INT'),
        Column('total_qty_reserve', 'INT'),
        Column('total_pct_reserve', 'DOUBLE'),
        Column('sku_pct', 'DOUBLE'),
        Column('group_id', 'INT'),
        Column('qty_allocated', 'INT'),
        Column('date_start', 'TIMESTAMP_NTZ(3)'),
        Column('date_end', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('dropship_retailer_id', 'INT'),
    ],
)
