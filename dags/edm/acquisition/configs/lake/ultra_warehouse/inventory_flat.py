from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultrawarehouse',
    schema='dbo',
    table='inventory_flat',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('inventory_flat_id', 'BIGINT', uniqueness=True),
        Column('inventory_id', 'BIGINT'),
        Column('warehouse_id', 'INT'),
        Column('location_id', 'INT'),
        Column('container_id', 'INT'),
        Column('case_id', 'BIGINT'),
        Column('lpn_id', 'BIGINT'),
        Column('item_id', 'INT'),
        Column('quantity', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
