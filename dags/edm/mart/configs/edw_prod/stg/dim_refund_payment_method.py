from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_dim import SnowflakeMartDimOperator


def get_mart_operator():
    return SnowflakeMartDimOperator(
        table='dim_refund_payment_method',
        use_surrogate_key=True,
        initial_load_value='1900-01-01',
        transform_proc_list=['stg.transform_dim_refund_payment_method.sql'],
        column_list=[
            Column('refund_payment_method', 'VARCHAR(25)', uniqueness=True),
            Column('refund_payment_method_type', 'VARCHAR(25)', uniqueness=True),
            Column('source_refund_payment_method', 'VARCHAR(25)'),
        ],
        watermark_tables=['lake_consolidated.ultra_merchant.refund'],
    )
