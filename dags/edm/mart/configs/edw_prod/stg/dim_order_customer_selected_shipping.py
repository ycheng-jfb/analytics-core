from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_dim import SnowflakeMartDimOperator


def get_mart_operator():
    return SnowflakeMartDimOperator(
        table='dim_order_customer_selected_shipping',
        initial_load_value='1900-01-01',
        transform_proc_list=['stg.transform_dim_order_customer_selected_shipping.sql'],
        use_surrogate_key=True,
        column_list=[
            Column('customer_selected_shipping_type', 'VARCHAR', uniqueness=True),
            Column('customer_selected_shipping_service', 'VARCHAR', uniqueness=True),
            Column('customer_selected_shipping_description', 'VARCHAR', uniqueness=True),
            Column('customer_selected_shipping_price', 'NUMBER(38,4)', uniqueness=True),
        ],
        watermark_tables=[
            'lake_consolidated.ultra_merchant.shipping_option',
            'lake_consolidated.ultra_merchant.order_shipping',
        ],
    )
