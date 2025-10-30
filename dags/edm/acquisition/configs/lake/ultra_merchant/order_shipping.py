from include.airflow.operators.mssql_acquisition import HighWatermarkMax
from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='order_shipping',
    watermark_column='order_shipping_id',
    initial_load_value='-1',
    high_watermark_cls=HighWatermarkMax,
    strict_inequality=True,
    enable_archive=False,
    schema_version_prefix='v2',
    column_list=[
        Column('order_shipping_id', 'INT', uniqueness=True, delta_column=True),
        Column('order_id', 'INT'),
        Column('order_line_id', 'INT'),
        Column('order_offer_id', 'INT'),
        Column('shipping_option_id', 'INT'),
        Column('carrier_service_id', 'INT'),
        Column('carrier_rate_id', 'INT'),
        Column('level', 'VARCHAR(15)'),
        Column('type', 'VARCHAR(15)'),
        Column('cost', 'NUMBER(19, 4)'),
        Column('amount', 'NUMBER(19, 4)'),
    ],
)
