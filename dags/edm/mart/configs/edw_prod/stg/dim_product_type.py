from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_dim import SnowflakeMartDimOperator


def get_mart_operator():
    return SnowflakeMartDimOperator(
        table="dim_product_type",
        use_surrogate_key=True,
        initial_load_value="1900-01-01",
        transform_proc_list=["stg.transform_dim_product_type.sql"],
        column_list=[
            Column("product_type_id", "INT", uniqueness=True),
            Column("product_type_name", "VARCHAR(50)"),
            Column("is_free", "BOOLEAN"),
            Column("source_is_free", "BOOLEAN"),
        ],
        watermark_tables=["lake_consolidated.ultra_merchant.product_type"],
    )
