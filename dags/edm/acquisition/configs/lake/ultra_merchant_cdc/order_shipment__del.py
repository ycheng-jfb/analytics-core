from include.airflow.operators.mssql_acquisition import HighWatermarkMaxRowVersion
from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='um_replicated',
    schema='dbo',
    table='ultramerchant__order_shipment__del',
    watermark_column='repl_timestamp',
    initial_load_value='0x0',
    high_watermark_cls=HighWatermarkMaxRowVersion,
    schema_version_prefix='v2',
    column_list=[
        Column('order_shipment_id', 'INT'),
        Column('order_id', 'INT'),
        Column('warehouse_id', 'INT'),
        Column('shipping_address_id', 'INT'),
        Column('date_shipped', 'TIMESTAMP_NTZ(0)'),
        Column('carrier_id', 'INT'),
        Column('carrier_service_id', 'INT'),
        Column('zone', 'INT'),
        Column('rate', 'NUMBER(19, 4)'),
        Column('rate_weight', 'DOUBLE'),
        Column('tracking_number', 'VARCHAR(50)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('statuscode', 'INT'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('delivered', 'INT'),
        Column('date_delivered', 'TIMESTAMP_NTZ(0)'),
        Column('date_estimate_delivery', 'TIMESTAMP_NTZ(3)'),
        Column('repl_action', 'VARCHAR(1)'),
        Column('repl_timestamp', 'BINARY(8)', uniqueness=True),
    ],
)
