from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_dim import SnowflakeMartDimOperator


def get_mart_operator():
    return SnowflakeMartDimOperator(
        table='dim_chargeback_reason',
        use_surrogate_key=True,
        initial_load_value='1900-01-01',
        transform_proc_list=['stg.transform_dim_chargeback_reason.sql'],
        column_list=[Column('chargeback_reason', 'VARCHAR(255)', uniqueness=True)],
        watermark_tables=[
            'lake_consolidated.ultra_merchant.psp_notification_log',
            'lake_consolidated.ultra_merchant.order_chargeback_log',
            'lake.oracle_ebs.chargeback_eu',
            'lake.oracle_ebs.chargeback_us',
        ],
    )
