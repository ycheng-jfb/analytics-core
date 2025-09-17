from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_dim import SnowflakeMartDimOperator


def get_mart_operator():
    return SnowflakeMartDimOperator(
        table="dim_discount_type",
        use_surrogate_key=False,
        initial_load_value="1900-01-01",
        transform_proc_list=["stg.transform_dim_discount_type.sql"],
        column_list=[
            Column("discount_type_id", "NUMBER(38,0)", uniqueness=True),
            Column("discount_type_label", "VARCHAR(50)"),
            Column("discount_type_description", "VARCHAR(255)"),
        ],
        watermark_tables=[
            "lake_consolidated.ultra_merchant.discount_type",
        ],
    )
