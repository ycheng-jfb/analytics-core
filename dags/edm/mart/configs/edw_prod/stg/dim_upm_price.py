from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_dim import SnowflakeMartDimOperator


def get_mart_operator():
    return SnowflakeMartDimOperator(
        table="dim_upm_price",
        use_surrogate_key=False,
        initial_load_value="1900-01-01",
        transform_proc_list=["stg.transform_dim_upm_price.sql"],
        column_list=[
            Column("price_id", "INT", uniqueness=True),
            Column("color_id", "INT"),
            Column("tfg_msrp", "FLOAT"),
            Column("tfg_vip_price", "FLOAT"),
        ],
        watermark_tables=[
            "lake.centric.ed_price",
        ],
    )
