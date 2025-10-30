from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_dim import SnowflakeMartDimOperator


def get_mart_operator():
    return SnowflakeMartDimOperator(
        table='dim_refund_status',
        use_surrogate_key=True,
        initial_load_value='1900-01-01',
        transform_proc_list=['stg.transform_dim_refund_status.sql'],
        column_list=[
            Column('refund_status_code', 'INT', uniqueness=True),
            Column('refund_status', 'VARCHAR(50)', uniqueness=True),
        ],
        watermark_tables=[
            'lake_consolidated.ultra_merchant.statuscode',
            'lake_consolidated.ultra_merchant.statuscode_category',
        ],
    )
