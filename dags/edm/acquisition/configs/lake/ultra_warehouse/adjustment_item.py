from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultrawarehouse',
    schema='dbo',
    table='adjustment_item',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('adjustment_item_id', 'INT', uniqueness=True),
        Column('adjustment_id', 'INT'),
        Column('item_id', 'INT'),
        Column('quantity', 'INT'),
        Column('quantity_allocated', 'INT'),
        Column('quantity_backordered', 'INT'),
        Column('quantity_cancelled', 'INT'),
        Column('quantity_completed', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('position', 'INT'),
    ],
)
