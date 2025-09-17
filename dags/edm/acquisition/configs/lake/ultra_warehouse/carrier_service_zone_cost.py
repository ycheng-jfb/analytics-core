from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultrawarehouse',
    schema='dbo',
    table='carrier_service_zone_cost',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('carrier_service_zone_cost_id', 'INT', uniqueness=True),
        Column('carrier_service_id', 'INT'),
        Column('weight_min', 'DOUBLE'),
        Column('weight_max', 'DOUBLE'),
        Column('shipping_zone', 'INT'),
        Column('cost', 'DOUBLE'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
