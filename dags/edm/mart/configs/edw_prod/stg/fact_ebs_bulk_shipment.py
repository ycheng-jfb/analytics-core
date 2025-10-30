from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_fact import SnowflakeMartFactOperator


def get_mart_operator():
    return SnowflakeMartFactOperator(
        table='fact_ebs_bulk_shipment',
        initial_load_value='1900-01-01',
        transform_proc_list=['stg.transform_fact_ebs_bulk_shipment.sql'],
        use_surrogate_key=False,
        column_list=[
            Column('transaction_id', 'NUMBER(38,0)', uniqueness=True),
            Column('item_number', 'VARCHAR(255)', uniqueness=True),
            Column('business_unit', 'VARCHAR(255)', uniqueness=True),
            Column('item_id', 'NUMBER(38,0)'),
            Column('product_id', 'NUMBER(38,0)'),
            Column('facility', 'VARCHAR(255)'),
            Column('sales_order_id', 'NUMBER(38,0)'),
            Column('line_number', 'NUMBER(38,0)'),
            Column('from_sub_inventory', 'VARCHAR(255)'),
            Column('shipped_quantity', 'NUMBER(38,0)'),
            Column('system_datetime', 'TIMESTAMP_TZ(3)'),
            Column('shipment_local_datetime', 'TIMESTAMP_TZ(3)'),
        ],
        watermark_tables=[
            'lake.ultra_warehouse.inventory_log',
            'lake.ultra_warehouse.inventory_log_container_item',
            'lake.ultra_warehouse.item',
            'lake.ultra_warehouse.lpn',
            'lake.ultra_warehouse.case',
            'lake.ultra_warehouse.location',
            'lake.ultra_warehouse.warehouse',
            'lake.ultra_warehouse.zone',
            'lake.ultra_warehouse.bulk_shipment_container_detail',
        ],
    )
