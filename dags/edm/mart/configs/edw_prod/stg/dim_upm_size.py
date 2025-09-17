from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_dim import SnowflakeMartDimOperator


def get_mart_operator():
    return SnowflakeMartDimOperator(
        table="dim_upm_size",
        use_surrogate_key=False,
        initial_load_value="1900-01-01",
        transform_proc_list=["stg.transform_dim_upm_size.sql"],
        column_list=[
            Column("size_range_id", "INT", uniqueness=True),
            Column("size_id", "INT", uniqueness=True),
            Column("size_range_description", "VARCHAR(300)"),
            Column("size", "VARCHAR(100)"),
        ],
        watermark_tables=[
            "lake.centric.ed_size_range",
            "lake.centric.er_size_range",
            "lake.centric.ed_product_size",
        ],
    )
