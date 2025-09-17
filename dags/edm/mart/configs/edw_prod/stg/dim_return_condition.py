from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_dim import SnowflakeMartDimOperator


def get_mart_operator():
    return SnowflakeMartDimOperator(
        table="dim_return_condition",
        use_surrogate_key=True,
        initial_load_value="1900-01-01",
        transform_proc_list=["stg.transform_dim_return_condition.sql"],
        column_list=[
            Column("return_condition", "VARCHAR(10)", uniqueness=True),
            Column("return_disposition", "VARCHAR(50)", uniqueness=True),
        ],
        watermark_tables=["lake_consolidated.ultra_merchant.return_disposition"],
    )
