from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_dim import SnowflakeMartDimOperator


def get_mart_operator():
    return SnowflakeMartDimOperator(
        table='dim_upm_fabric',
        use_surrogate_key=False,
        initial_load_value='1900-01-01',
        transform_proc_list=['stg.transform_dim_upm_fabric.sql'],
        column_list=[
            Column('style_id', 'INT', uniqueness=True),
            Column('fabric_id', 'INT', uniqueness=True),
            Column('fabric', 'VARCHAR(300)'),
            Column('fabric_family', 'VARCHAR(100)'),
            Column('opacity', 'VARCHAR(100)'),
        ],
        watermark_tables=[
            'lake.centric.er_style',
            'lake.centric.ed_material',
        ],
    )
