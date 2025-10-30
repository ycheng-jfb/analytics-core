from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultrawarehouse',
    schema='dbo',
    table='receipt_item_container',
    watermark_column='datetime_modified',
    column_list=[
        Column('receipt_item_container_id', 'INT', uniqueness=True),
        Column('receipt_item_id', 'INT'),
        Column('parent_container_id', 'INT'),
        Column('container_id', 'INT'),
        Column('case_id', 'INT'),
        Column('quantity', 'INT'),
        Column('quantity_cancelled', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('lpn_id', 'INT'),
    ],
)
