from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_dim import SnowflakeMartDimOperator


def get_mart_operator():
    return SnowflakeMartDimOperator(
        table='dim_return_reason',
        use_surrogate_key=False,
        initial_load_value='1900-01-01',
        transform_proc_list=['stg.transform_dim_return_reason.sql'],
        column_list=[
            Column('return_reason_id', 'INT', uniqueness=True),
            Column('return_reason', 'VARCHAR(100)'),
        ],
        watermark_tables=['lake_consolidated.ultra_merchant.return_reason'],
    )
