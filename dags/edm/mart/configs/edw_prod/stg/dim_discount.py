from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_dim import SnowflakeMartDimOperator


def get_mart_operator():
    return SnowflakeMartDimOperator(
        table='dim_discount',
        use_surrogate_key=False,
        initial_load_value='1900-01-01',
        transform_proc_list=['stg.transform_dim_discount.sql'],
        column_list=[
            Column('discount_id', 'NUMBER(38,0)', uniqueness=True),
            Column('meta_original_discount_id', 'NUMBER(38,0)'),
            Column('discount_applied_to', 'VARCHAR(15)'),
            Column('discount_calculation_method', 'VARCHAR(25)'),
            Column('discount_label', 'VARCHAR(50)'),
            Column('discount_percentage', 'NUMBER(18, 6)'),
            Column('discount_rate', 'NUMBER(19, 4)'),
            Column('discount_date_expires', 'TIMESTAMP_NTZ(0)'),
            Column('discount_status_code', 'INT'),
        ],
        watermark_tables=[
            'lake_consolidated.ultra_merchant.discount',
        ],
    )
