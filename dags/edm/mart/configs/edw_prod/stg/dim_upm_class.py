from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_dim import SnowflakeMartDimOperator


def get_mart_operator():
    return SnowflakeMartDimOperator(
        table='dim_upm_class',
        use_surrogate_key=False,
        initial_load_value='1900-01-01',
        transform_proc_list=['stg.transform_dim_upm_class.sql'],
        column_list=[
            Column('style_id', 'INT', uniqueness=True),
            Column('dept_id', 'INT'),
            Column('department', 'VARCHAR(100)'),
            Column('subdept_id', 'INT'),
            Column('subdepartment', 'VARCHAR(100)'),
            Column('class_id', 'INT'),
            Column('class', 'VARCHAR(100)'),
            Column('subclass_id', 'INT'),
            Column('subclass', 'VARCHAR(100)'),
        ],
        watermark_tables=[
            'lake.centric.ed_style',
            'lake.centric.ed_classifier0',
            'lake.centric.ed_classifier1',
            'lake.centric.ed_classifier2',
            'lake.centric.ed_classifier3',
        ],
    )
