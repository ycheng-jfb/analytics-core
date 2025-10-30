from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultrawarehouse',
    schema='dbo',
    table='las_po',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('las_po_id', 'INT', uniqueness=True),
        Column('vendor_id', 'VARCHAR(255)'),
        Column('po_number', 'VARCHAR(255)'),
        Column('ship_to_warehouse_id', 'INT'),
        Column('total_ordered', 'INT'),
        Column('total_cancelled', 'INT'),
        Column('total_sku_printed', 'INT'),
        Column('total_lpn_printed', 'INT'),
        Column('total_case_printed', 'INT'),
        Column('total_case_packed', 'INT'),
        Column('total_case_intransit', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('type_code_id', 'INT'),
        Column('total_case_floorset', 'INT'),
    ],
)
