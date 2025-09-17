from include.airflow.operators.snowflake import SnowflakeEdwProcedureOperator


def get_mart_operator():
    return SnowflakeEdwProcedureOperator(
        procedure="reference.na_outbound_shipping_cost.sql", database="edw_prod"
    )
