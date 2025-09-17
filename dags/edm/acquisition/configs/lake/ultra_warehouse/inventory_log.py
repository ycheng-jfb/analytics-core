from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultrawarehouse',
    schema='dbo',
    table='inventory_log',
    watermark_column='datetime_modified',
    initial_load_value='2020-01-14',
    schema_version_prefix='v2',
    column_list=[
        Column('inventory_log_id', 'INT', uniqueness=True),
        Column('parent_inventory_log_id', 'INT'),
        Column('type_code_id', 'INT'),
        Column('warehouse_id', 'INT'),
        Column('location_id', 'INT'),
        Column('parent_container_id', 'INT'),
        Column('container_id', 'INT'),
        Column('case_id', 'INT'),
        Column('lpn_id', 'INT'),
        Column('item_id', 'INT'),
        Column('quantity', 'INT'),
        Column('object', 'VARCHAR(75)'),
        Column('object_id', 'INT'),
        Column('administrator_id', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('datetime_transaction', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_local_transaction', 'TIMESTAMP_NTZ(3)'),
    ],
)
