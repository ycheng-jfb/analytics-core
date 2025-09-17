from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultrawarehouse',
    schema='dbo',
    table='in_transit_container',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('in_transit_container_id', 'INT', uniqueness=True),
        Column('in_transit_id', 'INT'),
        Column('label', 'VARCHAR(255)'),
        Column('seal_number', 'VARCHAR(255)'),
        Column('tracking_number', 'VARCHAR(255)'),
        Column('datetime_shipped', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_estimated_delivery', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_delivered', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_cancelled', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('status_code_id', 'INT'),
        Column('container_weight', 'DOUBLE'),
        Column('equipment_type', 'VARCHAR(255)'),
        Column('carton_count', 'INT'),
        Column('traffic_mode', 'VARCHAR(255)'),
    ],
)
