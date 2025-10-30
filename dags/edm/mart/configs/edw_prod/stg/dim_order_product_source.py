from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_dim import SnowflakeMartDimOperator


def get_mart_operator():
    return SnowflakeMartDimOperator(
        table='dim_order_product_source',
        use_surrogate_key=True,
        initial_load_value='1900-01-01',
        transform_proc_list=['stg.transform_dim_order_product_source.sql'],
        column_list=[
            Column('order_product_source_name', 'VARCHAR(200)', uniqueness=True, allow_nulls=False),
        ],
        watermark_tables=['lake_consolidated.ultra_merchant.order_product_source'],
    )
