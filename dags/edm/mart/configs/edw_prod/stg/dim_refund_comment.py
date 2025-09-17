from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_dim import SnowflakeMartDimOperator


def get_mart_operator():
    return SnowflakeMartDimOperator(
        table="dim_refund_comment",
        use_surrogate_key=True,
        initial_load_value="1900-01-01",
        transform_proc_list=["stg.transform_dim_refund_comment.sql"],
        column_list=[Column("refund_comment", "VARCHAR(255)", uniqueness=True)],
        watermark_tables=["lake_consolidated.ultra_merchant.refund"],
    )
