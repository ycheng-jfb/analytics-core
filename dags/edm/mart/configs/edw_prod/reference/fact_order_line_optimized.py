from include.airflow.operators.snowflake import SnowflakeEdwProcedureOperator


def get_mart_operator():
    return SnowflakeEdwProcedureOperator(
        procedure="reference.fact_order_line_optimized.sql", database="edw_prod"
    )
