from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='order_line_warehouse_mapping_log',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('order_line_warehouse_mapping_log_id', 'INT', uniqueness=True),
        Column('order_id', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('try_count', 'INT'),
        Column('favored_warehouse_id', 'INT'),
    ],
)
