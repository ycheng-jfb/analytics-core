from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_fact import SnowflakeMartFactOperator


def get_mart_operator():
    return SnowflakeMartFactOperator(
        table='fact_inventory_in_transit_history',
        initial_load_value='1900-01-01',
        transform_proc_list=['stg.transform_fact_inventory_in_transit_history.sql'],
        use_surrogate_key=False,
        column_list=[
            Column('item_id', 'NUMBER(38,0)', uniqueness=True),
            Column('rollup_date', 'DATE', uniqueness=True),
            Column('sku', 'VARCHAR(30)'),
            Column('from_warehouse_id', 'NUMBER(38,0)', uniqueness=True),
            Column('to_warehouse_id', 'NUMBER(38,0)', uniqueness=True),
            Column('units', 'NUMBER(38,0)'),
            Column('is_deleted', 'BOOLEAN'),
            Column('is_current', 'BOOLEAN'),
        ],
        watermark_tables=[
            'lake.ultra_warehouse.item',
            'lake.ultra_warehouse.lpn',
            'lake.ultra_warehouse.inventory',
            'lake.ultra_warehouse.inventory_location',
            'lake.ultra_warehouse.location',
            'lake.ultra_warehouse.zone',
            'lake.ultra_warehouse.code',
        ],
    )
