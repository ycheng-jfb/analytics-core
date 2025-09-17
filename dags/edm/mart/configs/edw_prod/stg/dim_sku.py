from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_dim import SnowflakeMartDimOperator


def get_mart_operator():
    return SnowflakeMartDimOperator(
        table='dim_sku',
        use_surrogate_key=True,
        initial_load_value='1900-01-01',
        transform_proc_list=['stg.transform_dim_sku.sql'],
        column_list=[
            Column('sku', 'VARCHAR(30)', uniqueness=True),
            Column('product_sku', 'VARCHAR(30)'),
            Column('base_sku', 'VARCHAR(30)'),
        ],
        watermark_tables=[
            'lake_consolidated.ultra_merchant.item',
            'lake.ultra_warehouse.item',
        ],
    )
