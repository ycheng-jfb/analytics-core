from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultrawarehouse',
    schema='dbo',
    table='item_warehouse_region',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('item_warehouse_region_id', 'INT', uniqueness=True),
        Column('region_id', 'INT'),
        Column('item_id', 'INT'),
        Column('total_onhand', 'INT'),
        Column('sellable_onhand', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
