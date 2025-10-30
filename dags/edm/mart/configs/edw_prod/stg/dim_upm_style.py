from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_dim import SnowflakeMartDimOperator


def get_mart_operator():
    return SnowflakeMartDimOperator(
        table='dim_upm_style',
        use_surrogate_key=False,
        initial_load_value='1900-01-01',
        transform_proc_list=['stg.transform_dim_upm_style.sql'],
        column_list=[
            Column('style_id', 'INT', uniqueness=True),
            Column('centric_style_id', 'VARCHAR(50)'),
            Column('actual_size_range', 'INT'),
            Column('tfg_collection', 'INT'),
            Column('gender', 'VARCHAR(50)'),
            Column('style_name', 'VARCHAR(300)'),
            Column('style_number_po', 'VARCHAR(100)'),
            Column('tfg_rank', 'VARCHAR(50)'),
            Column('tfg_core_fashion', 'VARCHAR(150)'),
            Column('coverage', 'VARCHAR(100)'),
        ],
        watermark_tables=[
            'lake.centric.ed_style',
        ],
    )
