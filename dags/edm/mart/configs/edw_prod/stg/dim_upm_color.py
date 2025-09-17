from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_dim import SnowflakeMartDimOperator


def get_mart_operator():
    return SnowflakeMartDimOperator(
        table="dim_upm_color",
        use_surrogate_key=False,
        initial_load_value="1900-01-01",
        transform_proc_list=["stg.transform_dim_upm_color.sql"],
        column_list=[
            Column("color_id", "INT", uniqueness=True),
            Column("style_id", "INT"),
            Column("site_color", "VARCHAR(100)"),
            Column("color", "VARCHAR(100)"),
            Column("color_family", "VARCHAR(100)"),
            Column("color_value", "VARCHAR(100)"),
            Column("pattern", "VARCHAR(100)"),
        ],
        watermark_tables=[
            "lake.centric.ed_colorway",
            "lake.centric.ed_color_specification",
            "lake.centric.ed_print_design_color",
        ],
    )
